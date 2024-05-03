using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using SimioAPI;
using SimioAPI.Extensions;
using System.Data;
using System.Xml;
using Newtonsoft.Json;
using System.Runtime.Remoting.Contexts;



namespace AmazonS3GridData
{
    public class ImporterDefinition : IGridDataImporterDefinition
    {
        public string Name => "Amazon S3 Data Importer";
        public string Description => "An importer for Amazon S3 formatted data";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("8a6cd9fa-ee65-4652-b53d-ce6fd1b6ec83");
        public Guid UniqueID => MY_ID;

        public IGridDataImporter CreateInstance(IGridDataImporterContext context)
        {
            return new Importer(context);
        }
        public void DefineSchema(IGridDataSchema schema)
        {
            var regionalEndpont = schema.OverallProperties.AddListProperty("RegionalEndpoint", Utils.GetListOfRegionEndpoints());
            regionalEndpont.DisplayName = "RegionalEndpoint";
            regionalEndpont.Description = "RegionalEndpoint.";
            regionalEndpont.DefaultValue = String.Empty;

            var accessKeyID = schema.OverallProperties.AddStringProperty("AccessKeyID");
            accessKeyID.DisplayName = "AccessKeyID";
            accessKeyID.Description = "AccessKeyID.";
            accessKeyID.DefaultValue = String.Empty;

            var secretAccessKey = schema.OverallProperties.AddCredentialProperty("SecretAccessKey");
            secretAccessKey.DisplayName = "SecretAccessKey";
            secretAccessKey.Description = "SecretAccessKey.";
            secretAccessKey.DefaultValue = String.Empty;

            var bucketName = schema.PerTableProperties.AddStringProperty("BucketName");
            bucketName.DisplayName = "BucketName";
            bucketName.Description = "BucketName.";
            bucketName.DefaultValue = String.Empty;

            var keyName = schema.PerTableProperties.AddStringProperty("KeyName");
            keyName.DisplayName = "KeyName";
            keyName.Description = "KeyName.";
            keyName.DefaultValue = String.Empty;

            var messageType = schema.PerTableProperties.AddListProperty("MessageType", new[] { "JSON", "XML", "OTHER" });
            messageType.DisplayName = "MessageType";
            messageType.Description = "MessageType.";
            messageType.DefaultValue = "JSON";

            var stylesheetProp = schema.PerTableProperties.AddXSLTProperty("Stylesheet");
            stylesheetProp.Description = "The transform to apply to the data returned from the download.";
            stylesheetProp.DefaultValue =
@"<xsl:stylesheet version=""1.0"" xmlns:xsl=""http://www.w3.org/1999/XSL/Transform"">
    <xsl:template match=""node()|@*"">
      <xsl:copy>
        <xsl:apply-templates select=""node()|@*""/>
      </xsl:copy>
    </xsl:template>
</xsl:stylesheet>";
            stylesheetProp.GetXML += StylesheetProp_GetXML;
        }

        private void StylesheetProp_GetXML(object sender, XSLTAddInPropertyGetXMLEventArgs e)
        {
            string stylesheet = string.Empty;

            Importer.GetValues(e.HierarchicalProperties[0], e.OtherProperties, out var regionalEndpoint, out var accessKeyID, out var secretAccessKey, out var bucketName, out var keyName, out var messageType, ref stylesheet);

            Importer.GetData(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName, out var resultString);

            if (messageType == "OTHER")
            {
                resultString = "<data><![CDATA[" + resultString.TrimEnd().Replace("\\", String.Empty) + "]]></data>";
            }
            else if (messageType == "JSON")
            {
                resultString = JsonConvert.DeserializeXmlNode(resultString).InnerXml;
            }
            e.XML = resultString;            
        }
    }

    class Importer : IGridDataImporter
    {
        public Importer(IGridDataImporterContext context)
        {
        }

        public OpenImportDataResult OpenData(IGridDataOpenImportDataContext openContext)
        {
            string stylesheet = string.Empty;

            GetValues(openContext.Settings.Properties, openContext.Settings.GridDataSettings[openContext.TableName].Properties, out var regionalEndpoint, out var accessKeyID, out var secretAccessKey, out var bucketName, out var keyName, out var messageType, ref stylesheet);
            
            if (String.IsNullOrWhiteSpace(regionalEndpoint))
                return OpenImportDataResult.Failed("The Regional Endpoint parameter is not specified");
            
            if (String.IsNullOrWhiteSpace(accessKeyID))
                return OpenImportDataResult.Failed("The Access Key ID parameter is not specified");
            
            if (String.IsNullOrWhiteSpace(secretAccessKey))
                return OpenImportDataResult.Failed("The Secret Access Key parameter is not specified");

            if (String.IsNullOrWhiteSpace(bucketName))
                return OpenImportDataResult.Failed("The Bucket Name parameter is not specified");

            if (String.IsNullOrWhiteSpace(keyName))
                return OpenImportDataResult.Failed("The Key Name parameter is not specified");

            if (GetData(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName, out var resultString) == false)
            {
                throw new Exception(resultString);
            }

            var mergedDataSet = new DataSet();
            int numberOfRows = 0;

            if (messageType == "OTHER")
            {
                resultString = "<data><![CDATA[" + resultString.Replace("\"", string.Empty) + "]]></data>";
            }
            else
            {
                XmlDocument xmlDoc = new XmlDocument();
                if (messageType == "JSON")
                {
                    xmlDoc = JsonConvert.DeserializeXmlNode(resultString);
                }
                else
                {
                    xmlDoc.LoadXml(resultString);
                }
                resultString = xmlDoc.InnerXml;
            }

            var result = Simio.Xml.XsltTransform.TransformXmlToDataSet(resultString, stylesheet, null, out var finalXMLString);
            if (result.XmlTransformError != null)
                return new OpenImportDataResult() { Result = GridDataOperationResult.Failed, Message = result.XmlTransformError };
            if (result.DataSetLoadError != null)
                return new OpenImportDataResult() { Result = GridDataOperationResult.Failed, Message = result.DataSetLoadError };
            if (result.DataSet.Tables.Count > 0) numberOfRows = result.DataSet.Tables[0].Rows.Count;
            else numberOfRows = 0;
            if (numberOfRows > 0)
            {
                result.DataSet.AcceptChanges();
                if (mergedDataSet.Tables.Count == 0) mergedDataSet.Merge(result.DataSet);
                else mergedDataSet.Tables[0].Merge(result.DataSet.Tables[0]);
                mergedDataSet.AcceptChanges();
            }

            // If no rows found by importer, create result data table with zero rows, but the same set of columns from the table so importer does not error out saying "no column names in data source match existing column names in table"
            if (mergedDataSet.Tables.Count == 0)
            {
                var zeroRowTable = new DataTable();
                var columnSettings = openContext.Settings.GridDataSettings[openContext.TableName]?.ColumnSettings;
                if (columnSettings != null)
                {
                    foreach (var cs in columnSettings)
                    {
                        zeroRowTable.Columns.Add(cs.ColumnName);
                    }
                }
                mergedDataSet.Tables.Add(zeroRowTable);
            }

            return new OpenImportDataResult()
            {
                Result = GridDataOperationResult.Succeeded,
                Records = new AmazonS3GridDataRecords(mergedDataSet)
            };
            
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            string stylesheet = string.Empty;

            GetValues(context.Settings.Properties, context.Settings.GridDataSettings[context.GridDataName].Properties, out var regionalEndpoint, out var accessKeyID, out var secreteAccessKey, out var bucketName, out var keyName, out var messageType, ref stylesheet);

            if (String.IsNullOrWhiteSpace(regionalEndpoint) || String.IsNullOrWhiteSpace(bucketName) || String.IsNullOrWhiteSpace(keyName) || String.IsNullOrWhiteSpace(messageType)) 
                return null;

            return String.Format("Bound to Amazon S3 CSV: Region = {0}, Bucket = {1}, Key = {2}, Message Type = {3}", regionalEndpoint, bucketName, keyName, messageType);
        }

        internal static void GetValues(INamedSimioCollection<IAddInPropertyValue> overallSettings, INamedSimioCollection<IAddInPropertyValue> tableSettings, out string regionalEndpoint, out string accessKeyID, out string secretAccessKey, out string bucketName, out string keyName, out string messageType, ref string stylesheet)
        {
            regionalEndpoint = (string)overallSettings?["RegionalEndpoint"].Value;
            accessKeyID = (string)overallSettings?["AccessKeyID"].Value;
            secretAccessKey = (string)overallSettings?["SecretAccessKey"].Value;
            bucketName = (string)tableSettings?["BucketName"].Value;
            keyName = (string)tableSettings?["KeyName"].Value;
            messageType = (string)tableSettings?["MessageType"].Value;
            stylesheet = (string)tableSettings?["Stylesheet"].Value;
        }

        internal static bool GetData(string regionalEndpoint, string accessKeyID, string secretAccessKey, string bucketName, string keyName, out string resultString)
        {
            try
            {
                var ms = Utils.DownloadFile(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName);
                System.Diagnostics.Trace.TraceInformation("Success Downloading Data from : " + bucketName + "|" + keyName);
                resultString = Encoding.UTF8.GetString(ms.ToArray());
                return true;
            }
            catch (Exception ex)
            {
                resultString = ex.Message;
                return false;
            }
        }

        public void Dispose()
        {
        }
    }

    class AmazonS3GridDataRecords : IGridDataRecords
    {
        readonly DataSet _dataSet;

        public AmazonS3GridDataRecords(DataSet dataSet)
        {
            _dataSet = dataSet;
        }

        #region IGridDataRecords Members

        List<GridDataColumnInfo> _columnInfo;
        List<GridDataColumnInfo> ColumnInfo
        {
            get
            {
                if (_columnInfo == null)
                {
                    _columnInfo = new List<GridDataColumnInfo>();

                    if (_dataSet.Tables.Count > 0)
                    {
                        foreach (DataColumn dc in _dataSet.Tables[0].Columns)
                        {
                            var name = dc.ColumnName;
                            var type = dc.DataType;

                            _columnInfo.Add(new GridDataColumnInfo()
                            {
                                Name = name,
                                Type = type
                            });
                        }
                    }
                }

                return _columnInfo;
            }
        }

        public IEnumerable<GridDataColumnInfo> Columns
        {
            get { return ColumnInfo; }
        }

        #endregion

        #region IEnumerable<IGridDataRecord> Members


        public IEnumerator<IGridDataRecord> GetEnumerator()
        {
            if (_dataSet.Tables.Count > 0)
            {
                foreach (DataRow dr in _dataSet.Tables[0].Rows)
                {
                    yield return new AmazonS3GridDataRecord(dr);
                }

            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }

    class AmazonS3GridDataRecord : IGridDataRecord
    {
        private readonly DataRow _dr;
        public AmazonS3GridDataRecord(DataRow dr)
        {
            _dr = dr;
        }

        #region IGridDataRecord Members

        public string this[int index]
        {
            get
            {
                var theValue = _dr[index];

                // Simio will first try to parse dates in the current culture
                if (theValue is DateTime)
                    return ((DateTime)theValue).ToString();

                return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}", _dr[index]);
            }
        }

        #endregion
    }
}

