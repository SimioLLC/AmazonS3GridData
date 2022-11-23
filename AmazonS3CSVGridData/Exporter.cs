using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.IO;    
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonS3CSVGridData
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "Amazon S3 CSV Data Exporter";
        public string Description => "An exporter to Amazon S3 CSV formatted data";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("139bb867-81f5-4ead-bf5b-1f952e1315b0");
        public Guid UniqueID => MY_ID;

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var useHeaders = schema.OverallProperties.AddBooleanProperty("WriteHeaders");
            useHeaders.DisplayName = "Write Headers";
            useHeaders.Description = "Indicates if the CSV file should contain column headers";
            useHeaders.DefaultValue = true;

            var separator = schema.OverallProperties.AddStringProperty("Separator");
            separator.DisplayName = "Separator";
            separator.Description = "The type of separator used to separate values in the CSV file";
            separator.DefaultValue = ",";

            var culture = schema.OverallProperties.AddStringProperty("ExportCulture");
            culture.DisplayName = "Export Culture";
            culture.Description = "The optional specific culture to use to write values to the file. This is useful for number formats written for a culture different than the current one.";
            culture.DefaultValue = String.Empty;

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

            var debugFileFolder = schema.PerTableProperties.AddFilesLocationProperty("DebugFileFolder");
            debugFileFolder.DisplayName = "Debug File Folder";
            debugFileFolder.Description = "Debug File Folder.";
            debugFileFolder.DefaultValue = String.Empty;

            var enabledExport = schema.PerColumnProperties.AddBooleanProperty("EnableExport");
            enabledExport.DisplayName = "Enabled Export";
            enabledExport.Description = "If true, this column will be exported. If false, it will not.";
            enabledExport.DefaultValue = true;
        }
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            GetValues(openContext.GridDataName, openContext.Settings, out var regionalEndpoint, out var accessKeyID, out var secretAccessKey, out var bucketName, out var keyName, out var debugFileFolder, out var bUseHeaders, out var separator, out var culture);

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

            System.Globalization.CultureInfo cultureInfo = System.Globalization.CultureInfo.InvariantCulture;
            if (String.IsNullOrWhiteSpace(culture) == false)
            {
                try
                {
                    cultureInfo = System.Globalization.CultureInfo.GetCultureInfo(culture);
                }
                catch (Exception e)
                {
                    return OpenExportDataResult.Failed($"The culture '{culture ?? String.Empty}' is not valid. Message: {e.Message}");
                }
            }

            using (var ms = new System.IO.MemoryStream())
            using (var sw = new System.IO.StreamWriter(ms, Encoding.UTF8))
            { 
                int numColumns = openContext.Records.Columns.Count();

                // Get the column settings for the grid data we are currently exporting
                var columnSettings = openContext.Settings.GridDataSettings[openContext.GridDataName].ColumnSettings;

                // Create a boolean array matched up to the columns, indicating if they should be exported
                var columnsEnabledExport = openContext.Records.Columns
                    .Select(c => (bool)columnSettings[c.Name].Properties["EnableExport"].Value)
                    .ToArray();

                if (bUseHeaders)
                    sw.WriteLine(String.Join(separator, openContext.Records.Columns.Where((c, i) => columnsEnabledExport[i]).Select(c => c.Name)));
                    
                var sb = new StringBuilder();
                foreach (var record in openContext.Records)
                {
                    sb.Clear();

                    var valueWritten = false;

                    for (int i = 0; i < numColumns; i++)
                    {
                        if (columnsEnabledExport[i] == true)
                        {
                            // Put in the separator before the value, if it is not the first one
                            if (valueWritten)
                                sb.Append(separator);

                            var valueStr = record.GetString(i) ?? "null";

                            // Special handle certain types, to make sure they follow the export culture
                            var valueObj = record.GetNativeObject(i);
                            if (valueObj is DateTime)
                            {
                                valueStr = String.Format(cultureInfo, "{0}", valueObj);
                            }
                            else if (valueObj is double valueDouble)
                            {
                                if (Double.IsPositiveInfinity(valueDouble))
                                    valueStr = "Infinity";
                                else if (Double.IsNegativeInfinity(valueDouble))
                                    valueStr = "-Infinity";
                                else if (Double.IsNaN(valueDouble))
                                    valueStr = "NaN";
                                else
                                    valueStr = String.Format(cultureInfo, "{0}", valueDouble);
                            }

                            // Per https://www.ietf.org/rfc/rfc4180.txt, certain values need special handling
                            if (NeedsQuoted(valueStr, separator))
                            {
                                // Surround in quotes, and escape double-quotes (if any) in the value itself
                                valueStr = $"\"{valueStr.Replace("\"", "\"\"")}\"";
                            }
                            sb.Append(valueStr);
                            valueWritten = true;
                        }
                    }
                    sw.WriteLine(sb.ToString());
                }
                sw.Flush();
                ms.Position = 0;
                Utils.UploadFile(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName, ms);
                if (debugFileFolder.Length > 0)
                {
                    Utils.DownloadFileToFile(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName, debugFileFolder + "\\" + keyName);
                }
                System.Diagnostics.Trace.TraceInformation("Success Exporting Data to : " + bucketName + "|" + keyName);
            }

            return OpenExportDataResult.Succeeded();
        }

        private static bool NeedsQuoted(string csvValue, string separator)
        {
            // Any caller should pass a separator of at least 1 character
            System.Diagnostics.Debug.Assert(String.IsNullOrWhiteSpace(separator) == false);

            foreach(var c in csvValue)
            {
                if (separator.Length > 0 && c == separator[0])
                    return true;
                else if (c == '\"')
                    return true;
                else if (c == '\n')
                    return true;
            }

            if (separator.Length > 1) // It really shouldn't be, but just in case...
            {
                if (csvValue.Contains(separator))
                    return true;
            }

            return false;
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            GetValues(context.GridDataName, context.Settings, out var regionalEndpoint, out var accessKeyID, out var secreteAccessKey, out var bucketName, out var keyName, out var debugFileFolder, out var bUseHeaders, out var separator, out var culture);

            if (String.IsNullOrWhiteSpace(regionalEndpoint) || String.IsNullOrWhiteSpace(bucketName) || String.IsNullOrWhiteSpace(keyName))
                return null;

            if (String.IsNullOrWhiteSpace(culture) == false)
                return String.Format("Exporting to Amazon S3 CSV: Region = {0}, Bucket = {1}, Key = {2}, Write headers = {3}, Separator = '{4}', Culture='{5}'", regionalEndpoint, bucketName, keyName, bUseHeaders, separator, culture);

            return String.Format("Exporting to Amazon S3 CSV: Region = {0}, Bucket = {1}, Key = {2}, Write headers = {3}, Separator = '{4}'", regionalEndpoint, bucketName, keyName, bUseHeaders, separator);
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string regionalEndpoint, out string accessKeyID, out string secretAccessKey, out string bucketName, out string keyName, out string debugFileFolder, out bool bUseHeaders, out string separator, out string culture)
        {
            regionalEndpoint = (string)settings.Properties["RegionalEndpoint"].Value;
            accessKeyID = (string)settings.Properties["AccessKeyID"].Value;
            secretAccessKey = (string)settings.Properties["SecretAccessKey"].Value;
            bucketName = (string)settings.GridDataSettings[tableName].Properties["BucketName"].Value;
            keyName = (string)settings.GridDataSettings[tableName].Properties["KeyName"].Value;
            debugFileFolder = (string)settings.GridDataSettings[tableName].Properties["DebugFileFolder"].Value;
            bUseHeaders = (bool)settings.Properties["WriteHeaders"].Value;
            var separatorstr = (string)settings.Properties["Separator"].Value;
            if (String.IsNullOrWhiteSpace(separatorstr))
                separatorstr = ",";
            separator = separatorstr;
            culture = (string)settings.Properties["ExportCulture"].Value;
        }

        public void Dispose()
        {
        }
    }
}
