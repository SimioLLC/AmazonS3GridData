using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.IO;    
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Globalization;
using Newtonsoft.Json;
using System.Xml;

namespace AmazonS3GridData
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "Amazon S3 Data Exporter";
        public string Description => "An exporter to Amazon S3 formatted data";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("139bb867-81f5-4ead-bf5b-1f952e1315b0");
        public Guid UniqueID => MY_ID;

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
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

            var editXMLStylesheetInputFile = schema.PerTableProperties.AddFileProperty("EditXMLStylesheetInputFile");
            editXMLStylesheetInputFile.DisplayName = "Edit XML Stylesheet Input File";
            editXMLStylesheetInputFile.Description = "Edit XML Stylesheet Input File.";
            editXMLStylesheetInputFile.DefaultValue = String.Empty;
        }

        private void StylesheetProp_GetXML(object sender, XSLTAddInPropertyGetXMLEventArgs e)
        {
            string stylesheet = string.Empty;

            Exporter.GetValues(e.HierarchicalProperties[0], e.OtherProperties, out var regionalEndpoint, out var accessKeyID, out var secretAccessKey, out var bucketName, out var keyName, out var messageType, ref stylesheet, out var editXMLStylesheetInputFile);

            Console.WriteLine("\nReading blob from\n\t{0}\n", editXMLStylesheetInputFile);

            e.XML = File.ReadAllText(editXMLStylesheetInputFile);
        }
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            string stylesheet = string.Empty;

            GetValues(openContext.Settings.Properties, openContext.Settings.GridDataSettings[openContext.GridDataName].Properties, out var regionalEndpoint, out var accessKeyID, out var secretAccessKey, out var bucketName, out var keyName, out var messageType, ref stylesheet, out var editXMLStylesheetInputFile);

            if (String.IsNullOrWhiteSpace(regionalEndpoint))
                return OpenExportDataResult.Failed("The Regional Endpoint parameter is not specified");

            if (String.IsNullOrWhiteSpace(accessKeyID))
                return OpenExportDataResult.Failed("The Access Key ID parameter is not specified");

            if (String.IsNullOrWhiteSpace(secretAccessKey))
                return OpenExportDataResult.Failed("The Secret Access Key parameter is not specified");

            if (String.IsNullOrWhiteSpace(bucketName))
                return OpenExportDataResult.Failed("The Bucket Name parameter is not specified");

            if (String.IsNullOrWhiteSpace(keyName))
                return OpenExportDataResult.Failed("The Key Name parameter is not specified");

            if (messageType != "JSON" && messageType != "XML" && messageType != "OTHER")
                return OpenExportDataResult.Failed("Invalid Message Type");

            var dataTable = ConvertExportRecordsToDataTable(openContext.Records, openContext.GridDataName);
            DataSet dataSet = new DataSet();
            dataSet.Tables.Add(dataTable);
            var xmlString = dataSet.GetXml();

            var result = Simio.Xml.XsltTransform.TransformXmlToDataSet(xmlString, stylesheet, null, out var finalXMLString);

            if (messageType == "JSON")
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.LoadXml(finalXMLString.Replace("\r\n", String.Empty).Trim());
                XmlElement root = xmlDoc.DocumentElement;
                finalXMLString = JsonConvert.SerializeXmlNode(xmlDoc.FirstChild);
            }

            using (var ms = new System.IO.MemoryStream())
            using (var sw = new System.IO.StreamWriter(ms))
            {
                //dataSet.WriteXml(sw);
                sw.WriteLine(finalXMLString);
                sw.Flush();
                ms.Position = 0;

                Utils.UploadFile(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName, ms);
                System.Diagnostics.Trace.TraceInformation("Success Exporting Data to : " + bucketName + "|" + keyName);
            }
                      
            return OpenExportDataResult.Succeeded();
        }

       

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            string stylesheet = string.Empty;

            GetValues(context.Settings.Properties, context.Settings.GridDataSettings[context.GridDataName].Properties, out var regionalEndpoint, out var accessKeyID, out var secreteAccessKey, out var bucketName, out var keyName, out var messageType, ref stylesheet, out var editXMLStylesheetInputFile);

            if (String.IsNullOrWhiteSpace(regionalEndpoint) || String.IsNullOrWhiteSpace(bucketName) || String.IsNullOrWhiteSpace(keyName))
                return null;

            return String.Format("Exporting to Amazon S3 CSV: Region = {0}, Bucket = {1}, Key = {2}, Message Type = {3}", regionalEndpoint, bucketName, keyName, messageType);
        }

        internal static void GetValues(INamedSimioCollection<IAddInPropertyValue> overallSettings, INamedSimioCollection<IAddInPropertyValue> tableSettings, out string regionalEndpoint, out string accessKeyID, out string secretAccessKey, out string bucketName, out string keyName, out string messageType, ref string stylesheet, out string editXMLStylesheetInputFile)
        {
            regionalEndpoint = (string)overallSettings?["RegionalEndpoint"].Value;
            accessKeyID = (string)overallSettings?["AccessKeyID"].Value;
            secretAccessKey = (string)overallSettings?["SecretAccessKey"].Value;
            bucketName = (string)tableSettings?["BucketName"].Value;
            keyName = (string)tableSettings?["KeyName"].Value;
            messageType = (string)tableSettings?["MessageType"].Value;
            stylesheet = (string)tableSettings?["Stylesheet"].Value;
            editXMLStylesheetInputFile = (string)tableSettings?["EditXMLStylesheetInputFile"].Value;
        }

        public void Dispose()
        {
        }
        
        internal static DataTable ConvertExportRecordsToDataTable(IGridDataExportRecords exportRecord, string tableName)
        {
            // New table
            var dataTable = new DataTable();
            dataTable.TableName = tableName;
            dataTable.Locale = CultureInfo.InvariantCulture;

            List<IGridDataExportColumnInfo> colImportLocalRecordsColumnInfoList = new List<IGridDataExportColumnInfo>();

            foreach (var col in exportRecord.Columns)
            {
                colImportLocalRecordsColumnInfoList.Add(col);
                var dtCol = dataTable.Columns.Add(col.Name, Nullable.GetUnderlyingType(col.Type) ?? col.Type);
            }

            // Add Rows to data table
            foreach (var record in exportRecord)
            {
                object[] thisRow = new object[dataTable.Columns.Count];

                int dbColIndex = 0;
                foreach (var colExportLocalRecordsColumnInfo in colImportLocalRecordsColumnInfoList)
                {
                    var valueObj = record.GetNativeObject(dbColIndex);
                    thisRow[dbColIndex] = valueObj;
                    dbColIndex++;
                }

                dataTable.Rows.Add(thisRow);
            }

            return dataTable;
        }
    }
}
