using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using SimioAPI;
using SimioAPI.Extensions;

using LumenWorks.Framework.IO.Csv;

namespace AmazonS3CSVGridData
{
    public class ImporterDefinition : IGridDataImporterDefinition
    {
        public string Name => "Amazon S3 CSV Data Importer";
        public string Description => "An importer for Amazon S3 CSV formatted data";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("8a6cd9fa-ee65-4652-b53d-ce6fd1b6ec83");
        public Guid UniqueID => MY_ID;

        public IGridDataImporter CreateInstance(IGridDataImporterContext context)
        {
            return new Importer(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var useHeaders = schema.OverallProperties.AddBooleanProperty("UseHeaders");
            useHeaders.DisplayName = "Use Headers";
            useHeaders.Description = "Indicates if the CSV file uses column headers";
            useHeaders.DefaultValue = true;

            var separator = schema.OverallProperties.AddStringProperty("Separator");
            separator.DisplayName = "Separator";
            separator.Description = "The type of separator used to separate values in the CSV file";
            separator.DefaultValue = ",";

            var culture = schema.OverallProperties.AddStringProperty("ImportCulture");
            culture.DisplayName = "Import Culture";
            culture.Description = "The optional specific culture to use to read values in from the file. This is useful for number formats coming from a culture different than the current one.";
            culture.DefaultValue = "";

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
        }
    }

    class Importer : IGridDataImporter
    {
        public Importer(IGridDataImporterContext context)
        {
        }

        public OpenImportDataResult OpenData(IGridDataOpenImportDataContext openContext)
        {
            GetValues(openContext.TableName, openContext.Settings, out var regionalEndpoint, out var accessKeyID, out var secretAccessKey, out var bucketName, out var keyName, out var debugFileFolder, out var bUseHeaders, out var separator, out var culture);

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

            System.Globalization.CultureInfo importerCulture = null;
            if (String.IsNullOrWhiteSpace(culture) == false)
            {
                try
                {
                    importerCulture = System.Globalization.CultureInfo.GetCultureInfo(culture);
                }
                catch
                {
                    importerCulture = null;
                }
            }

            if (debugFileFolder.Length > 0) Utils.DownloadFileToFile(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName, debugFileFolder + "\\" + keyName);
            var sr = Utils.DownloadFile(regionalEndpoint, accessKeyID, secretAccessKey, bucketName, keyName);
            System.Diagnostics.Trace.TraceInformation("Success Downloading Data from : " + bucketName + "|" + keyName);

            return new OpenImportDataResult()
            {
                Result = GridDataOperationResult.Succeeded,
                Records = new AmazonS3CSVGridDataRecords(sr, bUseHeaders, separator, importerCulture)
            };
            
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            GetValues(context.GridDataName, context.Settings, out var regionalEndpoint, out var accessKeyID, out var secreteAccessKey, out var bucketName, out var keyName, out var debugFileFolder, out var bUseHeaders, out var separator, out var culture);

            if (String.IsNullOrWhiteSpace(regionalEndpoint) || String.IsNullOrWhiteSpace(bucketName) || String.IsNullOrWhiteSpace(keyName)) 
                return null;

            if (String.IsNullOrWhiteSpace(culture) == false)
                return String.Format("Bound to Amazon S3 CSV: Region = {0}, Bucket = {1}, Key = {2}, Use headers = {3}, Separator = '{4}', Culture='{5}'", regionalEndpoint, bucketName, keyName, bUseHeaders, separator, culture);

            return String.Format("Bound to Amazon S3 CSV: Region = {0}, Bucket = {1}, Key = {2}, Use headers = {3}, Separator = '{4}'", regionalEndpoint, bucketName, keyName, bUseHeaders, separator);
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string regionalEndpoint, out string accessKeyID, out string secretAccessKey, out string bucketName, out string keyName, out string debugFileFolder, out bool bUseHeaders, out char separator, out string culture)
        {
            regionalEndpoint = (string)settings.Properties["RegionalEndpoint"].Value;
            accessKeyID = (string)settings.Properties["AccessKeyID"].Value;
            secretAccessKey = (string)settings.Properties["SecretAccessKey"].Value;
            bucketName = (string)settings.GridDataSettings[tableName].Properties["BucketName"].Value;
            keyName = (string)settings.GridDataSettings[tableName].Properties["KeyName"].Value;
            debugFileFolder = (string)settings.GridDataSettings[tableName].Properties["DebugFileFolder"].Value;
            bUseHeaders = (bool)settings.Properties["UseHeaders"].Value;
            var separatorstr = (string)settings.Properties["Separator"].Value;
            if (String.IsNullOrWhiteSpace(separatorstr))
                separatorstr = ",";
            separator = separatorstr[0];
            culture = (string)settings.Properties["ImportCulture"].Value;
        }

        public void Dispose()
        {
        }
    }

    class AmazonS3CSVGridDataRecords : IGridDataRecords
    {
        private StreamReader _sr;
        CachedCsvReader _reader;
        readonly bool _useHeaders;
        readonly char _separator;
        readonly System.Globalization.CultureInfo _culture;

        public AmazonS3CSVGridDataRecords(StreamReader sr, bool useHeaders, char separator, System.Globalization.CultureInfo culture)
        {
            _sr = sr;
            _useHeaders = useHeaders;
            _separator = separator;
            _culture = culture;
        }

        #region IGridDataRecords Members

        List<GridDataColumnInfo> _columnInfo;
        List<GridDataColumnInfo> ColumnInfo
        {
            get
            {
                if (_columnInfo == null)
                {
                    if (_sr == null)
                    {
                        return null;
                    }

                    _columnInfo = new List<GridDataColumnInfo>();

                    try
                    {
                        _sr.BaseStream.Position = 0;
                        _reader = new CachedCsvReader(_sr, false, _separator);
                        _reader.DefaultParseErrorAction = ParseErrorAction.AdvanceToNextLine;
                        _reader.MissingFieldAction = MissingFieldAction.ReplaceByEmpty;
                        _reader.SkipEmptyLines = true;

                        int fieldCount = _reader.FieldCount;

                        // Read in or create names for all columns
                        if (_useHeaders)
                        {
                            if (_reader.ReadNextRecord())
                            {
                                for (int i = 0; i < fieldCount; i++)
                                    _columnInfo.Add(new GridDataColumnInfo { Name = _reader[i], Type = typeof(string) });
                            }
                        }
                        else
                        {
                            for (int i = 0; i < fieldCount; i++)
                                _columnInfo.Add(new GridDataColumnInfo { Name = String.Format("Col{0}", i), Type = typeof(string) });
                        }

                        // Read in types from first data row
                        if (_reader.ReadNextRecord())
                        {
                            for (int i = 0; i < fieldCount; i++)
                            {
                                string value = _reader[i];

                                double dValue;
                                DateTime dtValue;

                                if (Double.TryParse(value, out dValue))
                                    _columnInfo[i] = new GridDataColumnInfo { Name = _columnInfo[i].Name, Type = typeof(double) };
                                else if (DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out dtValue))
                                    _columnInfo[i] = new GridDataColumnInfo { Name = _columnInfo[i].Name, Type = typeof(DateTime) };
                            }
                        }
                    }
                    catch (System.IO.IOException e)
                    {
                        Console.WriteLine($"Problem opening the csv file. Message: '{e.Message}'.\nStack trace:{e.StackTrace}");
                        return null;
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

        class AmazonS3CSVEnumerator : IEnumerator<IGridDataRecord>
        {
            CachedCsvReader _reader;
            System.Globalization.CultureInfo _importCultureInfo;

            public AmazonS3CSVEnumerator(CachedCsvReader reader, bool hasHeaders, char separator, System.Globalization.CultureInfo cultureInfo)
            {
                try
                {
                    _importCultureInfo = cultureInfo;
                     _reader = reader;
                    _reader.DefaultParseErrorAction = ParseErrorAction.AdvanceToNextLine;
                    _reader.MissingFieldAction = MissingFieldAction.ReplaceByEmpty;
                }
                catch (System.IO.IOException)
                {
                    _reader = null;
                }
            }

            #region IEnumerator<IGridDataRecord> Members

            IGridDataRecord _current;
            public IGridDataRecord Current
            {
                get { return _current; }
            }

            #endregion

            #region IDisposable Members

            public void Dispose()
            {
                if (_reader != null)
                {
                    _reader.Dispose();
                    _reader = null;
                }
            }

            #endregion

            #region IEnumerator Members

            object System.Collections.IEnumerator.Current
            {
                get { return _current; }
            }

            public bool MoveNext()
            {
                if (_reader != null && _reader.ReadNextRecord())
                {
                    _current = new AmazonS3CSVGridDataRecord(_reader, _importCultureInfo);
                    return true;
                }

                return false;
            }

            public void Reset()
            {
                if (_reader != null)
                    _reader.MoveTo(0);
            }

            #endregion
        }

        public IEnumerator<IGridDataRecord> GetEnumerator()
        {
            if (_reader == null)
                return null;

            _reader.MoveToStart();
            if (_useHeaders) _reader.ReadNextRecord();
            return new AmazonS3CSVEnumerator(_reader, _useHeaders, _separator, _culture);
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            if (_reader == null)
                return null;

            _reader.MoveToStart();
            if (_useHeaders) _reader.ReadNextRecord();
            return new AmazonS3CSVEnumerator(_reader, _useHeaders, _separator, _culture);
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }

    class AmazonS3CSVGridDataRecord : IGridDataRecord
    {
        readonly string[] _data;
        readonly System.Globalization.CultureInfo _importCultureInfo;

        public AmazonS3CSVGridDataRecord(CsvReader reader, System.Globalization.CultureInfo cultureInfo)
        {
            _importCultureInfo = cultureInfo;
            _data = new string[reader.FieldCount];
            reader.CopyCurrentRecordTo(_data);
        }

        #region IGridDataRecord Members

        public string this[int index]
        {
            get
            {
                if (_importCultureInfo != null && String.IsNullOrWhiteSpace(_data[index]) == false)
                {
                    double d = 0.0;
                    if (Double.TryParse(_data[index], System.Globalization.NumberStyles.Any, _importCultureInfo, out d))
                    {
                        // This parsed as a double in the import culture, but the reader expects things in the invariant culture
                        return d.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    }
                }
                return _data[index];
            }
        }

        #endregion
    }
}

