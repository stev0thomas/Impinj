// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

using System;
using System.Text;
using System.Text.RegularExpressions;

namespace ItemSenseRDBMService
{
    /// <summary>
    /// A class representing an SGTIN-96 data object
    /// </summary>
    public class Sgtin96
    {
        // The following constants defines the URI format prefix.
        private const string _uriPrefix = "urn:epc:tag:sgtin-96:";

        #region data members

        // _header value identifies Gen2 Tag Data Standard encoding scheme.
        // fixed at binary 00110000 for SGTIN-96.
        private const ushort _header = 48; // 8-bits

        // Filter value identifies the type of item being tagged
        //
        // Filter Value | Type
        // -----------------------------------------------------------
        //      0       | All Others
        //      1       | Point of Sale (POS) Trade Item
        //      2       | Full Case for Transport
        //      3       | Reserved
        //      4       | Inner Pack Trade Item Grouping for Handling
        //      5       | Reserved
        //      6       | Unit Load
        //      7       | Unit inside Trade Item or component inside a
        //                product not intended for individual sale
        // -----------------------------------------------------------
        //
        private ushort _filterValue = 1; // 3 bits; Default to Point of Sale Trade Item.
        private const ushort _filterValueMin = 0;
        private const ushort _filterValueMax = 7;

        // Partition field identifies how many bits are assigned to both the
        // company prefix and item reference fields.
        //
        // Partition value | Company Prefix Bits | Item Reference Bits
        // -----------------------------------------------------------
        //      0          |        40           |          4
        //      1          |        37           |          7
        //      2          |        34           |          10
        //      3          |        30           |          14
        //      4          |        27           |          17
        //      5          |        24           |          20
        //      6          |        20           |          24
        // -----------------------------------------------------------
        //
        private ushort _partition = 5; // 3 bits
        private const ushort _partitionValueMin = 0;
        private const ushort _partitionValueMax = 6;

        // Company prefix value
        private UInt64 _companyPrefix; // 20-40 bits; default 24-bits, per _partition
        private int _companyPrefixLengthInBits = 24;
        private int _companyPrefixLengthInDigits = 7;

        // Item reference value
        private UInt32 _itemReference; // 24-4 bits; default 20-bits, per _partition
        private int _itemReferenceLengthInBits = 20;
        private int _itemReferenceLengthInDigits = 6;

        // Item UPC
        private string _upc;
        private int _upcCheckDigit;

        // Serial number value
        private UInt64 _serialNumber = 0; // 38 bits
        private const int _serialNumberMaxBits = 38;

        private const ushort CONVERT_HEX = 16;
        private const ushort CONVERT_DECIMAL = 10;
        private const ushort CONVERT_BINARY = 2;
        #endregion

        #region public data members

        public ushort Header
        {
            get { return _header; }
        }

        public ushort FilterValue
        {
            get { return _filterValue; }
        }

        public ushort Partition
        {
            get { return _partition; }
        }

        public UInt64 CompanyPrefix
        {
            get { return _companyPrefix; }
        }

        public UInt32 ItemReference
        {
            get { return _itemReference; }
        }

        public string UPC
        {
            get { return _upc + _upcCheckDigit; }
        }

        public UInt64 SerialNumber
        {
            get { return _serialNumber; }
            set { _serialNumber = value; }
        }

        #endregion

        #region constructor

        /// <summary>
        /// The default constructor is defined as private to enforce
        /// the use of the static object creation methods
        /// </summary>
        private Sgtin96()
        {
        }

        #endregion

        #region public member functions

        /// <summary>
        /// Constructor that populates the Sgtin96 object with the contents
        /// of the provided string.
        /// </summary>
        /// <param name="Sgtin96AsString">
        /// SGTIN-96 in either URI:
        ///     urn:epc:tag:sgtin-96:1.0123456.012345.012345678901
        /// or EPC:
        ///     30340789000C0E42DFDC1C35
        /// format.
        /// </param>
        /// <exception cref="ArgumentException">
        /// Thrown when neither a URI or EPC format string are passed in.
        /// </exception>
        public static Sgtin96 FromString(string Sgtin96AsString)
        {
            Sgtin96 ReturnSgtin96 = null;

            if (String.IsNullOrEmpty(Sgtin96AsString))
            {
                throw new ArgumentNullException("Null or Empty SGTIN-96 string.");
            }

            if (true == IsValidUri(Sgtin96AsString))
            {
                // It is a URI string, so populate the object data members
                // accordingly
                try
                {
                    ReturnSgtin96 = FromSgtin96Uri(Sgtin96AsString);
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
            else if (true == IsValidEpc(Sgtin96AsString))
            {
                // It is an EPC string, so populate the object data members
                // accordingly
                try
                {
                    ReturnSgtin96 = FromSgtin96Epc(Sgtin96AsString);
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
            else
            {
                throw new ArgumentException("Invalid SGTIN-96 string");
            }

            return ReturnSgtin96;
        }

        /// <summary>
        /// Populate the current object with the data contained within
        /// the furnished Uri string
        /// </summary>
        /// <param name="Uri">
        /// An SGTIN-96 URI string, e.g. urn:epc:tag:sgtin-96:1.0123456.012345.012345678901
        /// </param>
        /// <exception cref="ArgumentException">
        /// Thrown when a malformed URI string is detected.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown when a null or empty string is detected.
        /// </exception>
        public static Sgtin96 FromSgtin96Uri(string Uri)
        {
            Sgtin96 ReturnSgtin96 = new Sgtin96();
            string LocalUri = Uri;
            string[] DataValues;

            if (IsValidUri(Uri))
            {
                // Strip out the fixed URI prefix string so that we can look at the data
                string DataFields = LocalUri.Replace(_uriPrefix, string.Empty);
                // Now, extract each of the remaining fields as a separate string
                DataValues = DataFields.Split('.');

                // Extract the Filter value
                ReturnSgtin96._filterValue = Convert.ToUInt16(DataValues[0]);
                // Extract the Company Prefix value
                ReturnSgtin96._companyPrefix = Convert.ToUInt64(DataValues[1]);
                // Extract the Item Reference Value
                ReturnSgtin96._itemReference = Convert.ToUInt32(DataValues[2]);
                // Calculate the Partition value from the number of
                // digits in the Item Reference field
                ReturnSgtin96._partition = (ushort)(DataValues[2].Length - 1);
                // Populate the Digit and Bit length fields for the
                // Company Prefix and Item Reference values
                SetLengthsFromPartition(ref ReturnSgtin96);
                // Extract the serial number
                ReturnSgtin96._serialNumber = Convert.ToUInt64(DataValues[3]);
                // Add company prefix digits, less the first '0' character,
                // which is added in the UPC to GTIN conversion
                string companyPrefixFormat = "D" + (ReturnSgtin96._companyPrefixLengthInDigits - 1).ToString();
                ReturnSgtin96._upc =
                    ReturnSgtin96._companyPrefix.ToString(companyPrefixFormat);
                // If the number of companyPrefix digits is less than 12, then
                // Add item reference digits, less the first '0' character,
                // which is added in the UPC to GTIN conversion
                if (12 > ReturnSgtin96._companyPrefixLengthInDigits)
                {
                    string itemPrefixFormat = "D" + (ReturnSgtin96._itemReferenceLengthInDigits - 1).ToString();
                    ReturnSgtin96._upc +=
                        ReturnSgtin96._itemReference.ToString(itemPrefixFormat);
                }

                CalculateUpcCheckDigit(ReturnSgtin96._upc, out ReturnSgtin96._upcCheckDigit);
            }
            else
            {
                throw new ArgumentException("Invalid SGTIN-96 URI");
            }

            return ReturnSgtin96;
        }

        /// <summary>
        /// Populate the current object with the data contained within
        /// the furnished Epc string
        /// </summary>
        /// <param name="Epc">
        /// A SGTIN-96 EPC string, e.g. 30340789000C0E42DFDC1C35
        /// </param>
        /// <exception cref="ArgumentException">
        /// Thrown when an EPC that is not 96-bits long is provided.
        /// </exception>
        public static Sgtin96 FromSgtin96Epc(string Epc)
        {
            Sgtin96 ReturnSgtin96 = new Sgtin96();
            string EpcToValidate = string.Empty;

            if (false == string.IsNullOrEmpty(Epc))
            {
                EpcToValidate = Epc.Replace(" ", string.Empty);
            }
            else
            {
                throw new ArgumentException("null SGTIN-96 EPC");
            }

            if (true == IsValidEpc(EpcToValidate))
            {
                string BinaryEpc = HexStringToBinString(EpcToValidate);

                // Extract the Filter Value
                ReturnSgtin96._filterValue = Convert.ToUInt16(BinaryEpc.Substring(8, 3), 2);
                // Extract the Partition Value
                ReturnSgtin96._partition = Convert.ToUInt16(BinaryEpc.Substring(11, 3), 2);
                // Populate the Digit and Bit length fields for the
                // Company Prefix and Item Reference values
                SetLengthsFromPartition(ref ReturnSgtin96);

                // Extract the Company Prefix
                ReturnSgtin96._companyPrefix =
                    Convert.ToUInt64(BinaryEpc.Substring(14, ReturnSgtin96._companyPrefixLengthInBits), 2);
                // Extract the Item Reference
                ReturnSgtin96._itemReference =
                    Convert.ToUInt32(BinaryEpc.Substring(14 + ReturnSgtin96._companyPrefixLengthInBits, ReturnSgtin96._itemReferenceLengthInBits), 2);
                // Extract the Serial number
                ReturnSgtin96._serialNumber =
                    Convert.ToUInt64(BinaryEpc.Substring(58, 38), 2);
                // Add company prefix digits, less the first '0' character,
                // which is added in the UPC to GTIN conversion
                string companyPrefixFormat = "D" + (ReturnSgtin96._companyPrefixLengthInDigits - 1).ToString();
                ReturnSgtin96._upc =
                    ReturnSgtin96._companyPrefix.ToString(companyPrefixFormat);
                // If the number of companyPrefix digits is less than 12, then
                // Add item reference digits, less the first '0' character,
                // which is added in the UPC to GTIN conversion
                if (12 > ReturnSgtin96._companyPrefixLengthInDigits)
                {
                    string itemPrefixFormat = "D" + (ReturnSgtin96._itemReferenceLengthInDigits - 1).ToString();
                    ReturnSgtin96._upc +=
                        ReturnSgtin96._itemReference.ToString(itemPrefixFormat);
                }

                // Now calculate the UPC check digit
                CalculateUpcCheckDigit(ReturnSgtin96._upc, out ReturnSgtin96._upcCheckDigit);
            }
            else
            {
                throw new ArgumentException("Invalid SGTIN-96 EPC");
            }

            return ReturnSgtin96;
        }

        /// <summary>
        /// Creates an Sgtin96 object from the provided UPC data according
        /// to the procedure outlined in the GS1 document
        /// "Translate a U.P.C. to a GTIN to an SGTIN to an EPC"
        /// Found on 01/02/2014 at:
        /// http://www.gs1us.org/DesktopModules/Bring2mind/DMX/Download.aspx?EntryId=361&Command=Core_Download&PortalId=0&TabId=73
        /// </summary>
        /// <param name="UPC">
        /// The Universal Product Code, as a string, to create an SGTIN-96 from.
        /// </param>
        /// <param name="companyPrefixLength">
        /// The length of the Company Prefix field in the UPC. Valid values are
        /// 6 to 10, inclusive.
        /// </param>
        /// <returns>
        /// A populated Sgtin96 object, without a serial number.
        /// </returns>
        public static Sgtin96 FromUPC(string UPC, int companyPrefixLength)
        {
            Sgtin96 ReturnSgtin96 = null;

            StringBuilder UriRepresentation = new StringBuilder();

            // Verify that the provided UPC is valid
            if (IsValidGtin(UPC))
            {
                // Build an appropriate URI string.
                // First, add the prefix:
                UriRepresentation.Append(_uriPrefix);
                // Append the filter value:
                UriRepresentation.Append("1.");
                // Extract the company prefix. As a '0' prefix is added to
                // make up the correct number of company prefix digits, 
                // per the GS1 standard, we extract one less character than
                // is defined by companyPrefixLength.
                // Ref: http://www.gs1us.org/DesktopModules/Bring2mind/DMX/Download.aspx?EntryId=361&Command=Core_Download&PortalId=0&TabId=73
                UriRepresentation.Append(UPC.Substring(0, companyPrefixLength - 1).PadLeft(companyPrefixLength, '0'));
                // Append the period delimiter:
                UriRepresentation.Append(".");
                // Add an Indicator Digit value of '0' to indicate item level
                // packaging.
                UriRepresentation.Append('0');
                // Add the Item Reference Number, skipping the check digit at the end
                UriRepresentation.Append(UPC.Substring(companyPrefixLength - 1, UPC.Length - (companyPrefixLength)));
                // Append a zero-value serial number
                UriRepresentation.Append(".0");
                // Now create SGTIN using the FromUri API:
                ReturnSgtin96 = Sgtin96.FromSgtin96Uri(UriRepresentation.ToString());
            }
            else
            {
                throw (new ArgumentException("Invalid UPC string."));
            }

            return ReturnSgtin96;
        }

        public static Sgtin96 FromGTIN(string Gtin, int companyPrefixLength)
        {
            Sgtin96 ReturnSgtin96 = new Sgtin96();
            string gtinLessFillerDigit = Gtin.Substring(1, Gtin.Length - 1);

            StringBuilder UriRepresentation = new StringBuilder();

            // Verify that the provided UPC is valid
            if (IsValidGtin(Gtin))
            {
                // Build an appropriate URI string.
                // First, add the prefix:
                UriRepresentation.Append(_uriPrefix);
                // Append the filter value:
                UriRepresentation.Append("1.");
                // Extract the company prefix
                UriRepresentation.Append(gtinLessFillerDigit.Substring(0, companyPrefixLength));
                // Append the period delimiter:
                UriRepresentation.Append(".");
                // Add an Indicator Digit value of '0' to indicate item level
                // packaging.
                UriRepresentation.Append('0');
                // Add the Item Reference Number, skipping the check digit at the end
                int ItemReferenceDigitCount = (gtinLessFillerDigit.Length - companyPrefixLength) - 1;
                UriRepresentation.Append(gtinLessFillerDigit.Substring(companyPrefixLength, ItemReferenceDigitCount));
                // Append a zero-value serial number
                UriRepresentation.Append(".0");
                // Now create SGTIN using the FromUri API:
                ReturnSgtin96 = Sgtin96.FromSgtin96Uri(UriRepresentation.ToString());
            }
            else
            {
                throw (new ArgumentException("Invalid GTIN string."));
            }

            return ReturnSgtin96;
        }

        /// <summary>
        /// Returns object contents in SGTIN-96 URI string format.
        /// </summary>
        /// <returns>
        /// SGTIN-96 URI string representation of object.
        /// </returns>
        public override string ToString()
        {
            return this.ToUri();
        }

        /// <summary>
        /// Returns object contents in SGTIN-96 URI string format.
        /// </summary>
        /// <returns>
        /// SGTIN-96 URI string representation of object.
        /// </returns>
        public string ToUri()
        {
            // Placeholder for building URI string
            StringBuilder SgtinUri = new StringBuilder();

            // Add the URI prefix
            SgtinUri.Append(_uriPrefix);
            // Append the Filter Value
            SgtinUri.Append(_filterValue);
            // Add the '.' delimiter
            SgtinUri.Append(".");

            // Append the Company Prefix
            SgtinUri.Append(_companyPrefix.ToString().PadLeft(_companyPrefixLengthInDigits, '0'));
            // Add the '.' delimiter
            SgtinUri.Append(".");
            // Append the Item Reference
            SgtinUri.Append(_itemReference.ToString().PadLeft(_itemReferenceLengthInDigits, '0'));
            // Add the '.' delimiter
            SgtinUri.Append(".");

            // Append the Serial Number
            SgtinUri.Append(_serialNumber.ToString());

            // Return the URI string
            return SgtinUri.ToString();
        }

        /// <summary>
        /// Returns object contents in SGTIN-96 EPC string format.
        /// </summary>
        /// <returns>
        /// SGTIN-96 EPC string representation of object.
        /// </returns>
        public string ToEpc()
        {
            // Placeholder for a binary representation of the EPC
            StringBuilder BinarySgtinEpc = new StringBuilder();

            // Add the SGTIN-96 header in binary format
            BinarySgtinEpc.Append((Convert.ToString(Convert.ToInt32(_header), CONVERT_BINARY).PadLeft(8, '0'))); ;
            // Append the Filter Value in binary format
            BinarySgtinEpc.Append((Convert.ToString(Convert.ToInt32(_filterValue), CONVERT_BINARY).PadLeft(3, '0')));
            // Append the Partition in binary format
            BinarySgtinEpc.Append((Convert.ToString(Convert.ToInt32(_partition), CONVERT_BINARY).PadLeft(3, '0')));
            // Append the Company Prefix
            BinarySgtinEpc.Append((Convert.ToString(Convert.ToInt64(_companyPrefix), CONVERT_BINARY).PadLeft(_companyPrefixLengthInBits, '0')));
            // Append the Item Reference
            BinarySgtinEpc.Append((Convert.ToString(Convert.ToInt32(_itemReference), CONVERT_BINARY).PadLeft(_itemReferenceLengthInBits, '0')));
            // Append the Serial Number
            BinarySgtinEpc.Append((Convert.ToString(Convert.ToInt64(_serialNumber), CONVERT_BINARY).PadLeft(38, '0')));

            // Return the EPC string in Hexadecimal format
            return BinStringToHexString(BinarySgtinEpc.ToString());
        }

        public string ToUpc()
        {
            // Placeholder for building URI string
            StringBuilder SgtinUpc = new StringBuilder();

            SgtinUpc.Append(_upc);
            SgtinUpc.Append(_upcCheckDigit.ToString());

            // Return the URI string
            return SgtinUpc.ToString();
        }

        /// <summary>
        /// Examines a string to determine whether it is a valid SGTIN in
        /// either URI or EPC form.
        /// </summary>
        /// <param name="testString">
        /// String to test.
        /// </param>
        /// <returns>
        /// True if the string is a valid SGTIN.
        /// False if the string is not a value SGTIN.
        /// </returns>
        public static bool IsValidSGTIN(string testString)
        {
            bool validSGTIN = false;

            if (false == String.IsNullOrEmpty(testString))
            {
                // A valid SGTIN has to pass scrutiny either a URI or an EPC
                validSGTIN = IsValidUri(testString) || IsValidEpc(testString);
            }

            return validSGTIN;
        }

        public static string GetGTIN(string inputSGTIN)
        {
            string returnGTIN = string.Empty;

            if (String.IsNullOrEmpty(inputSGTIN))
            {
                throw new ArgumentNullException("Null or Empty SGTIN-96 string.");
            }

            if (true == IsValidUri(inputSGTIN))
            {
                // It is a URI string, so populate the object data members
                // accordingly
                try
                {
                    Sgtin96 tempSgtin = FromSgtin96Uri(inputSGTIN);
                    tempSgtin._serialNumber = 0;
                    returnGTIN = tempSgtin.ToUri();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
            else if (true == IsValidEpc(inputSGTIN))
            {
                // It is an EPC string, so populate the object data members
                // accordingly
                try
                {
                    Sgtin96 tempSgtin = FromSgtin96Epc(inputSGTIN);
                    tempSgtin._serialNumber = 0;
                    returnGTIN = tempSgtin.ToUri();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
            else
            {
                throw new ArgumentException("Invalid SGTIN-96 string");
            }

            return returnGTIN;
        }

        /// <summary>
        /// Returns an SGTIN value with a zero value serial number.
        /// </summary>
        /// <returns>
        /// String URI representation of Sgtin96 object with
        /// zero value serial number.
        /// </returns>
        public string GetSGTINZeroValueSerialNumber()
        {
            string returnSgtin = string.Empty;

            ulong tempSerialNumber = _serialNumber;
            _serialNumber = 0;

            returnSgtin = this.ToUri();

            _serialNumber = tempSerialNumber;

            return returnSgtin;
        }

        /// <summary>
        /// Compares two Sgtin96 object data member values.
        /// </summary>
        /// <param name="obj">
        /// An Sgtin96 object to compare against.
        /// </param>
        /// <returns>
        /// true if data members of both objects match, false if they do not.
        /// </returns>
        public override bool Equals(System.Object obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // If parameter cannot be cast to Sgtin96 return false.
            Sgtin96 p = obj as Sgtin96;
            if ((System.Object)p == null)
            {
                return false;
            }

            // Return true if the fields match:
            return (_filterValue == p._filterValue) &&
                (_partition == p._partition) &&
                (_companyPrefix == p._companyPrefix) &&
                (_companyPrefixLengthInBits == p._companyPrefixLengthInBits) &&
                (_companyPrefixLengthInDigits == p._companyPrefixLengthInDigits) &&
                (_itemReference == p._itemReference) &&
                (_itemReferenceLengthInBits == p._itemReferenceLengthInBits) &&
                (_itemReferenceLengthInDigits == p._itemReferenceLengthInDigits) &&
                (_serialNumber == p._serialNumber);
        }

        /// <summary>
        /// Compares two Sgtin96 object data member values.
        /// </summary>
        /// <param name="a">First Sgtin96 object to compare</param>
        /// <param name="b">Second Sgtin96 object to compare</param>
        /// <returns>
        /// true if data members of both objects match, false if they do not.
        /// </returns>
        public static bool operator ==(Sgtin96 a, Sgtin96 b)
        {
            // If both are null, or both are same instance, return true.
            if (System.Object.ReferenceEquals(a, b))
            {
                return true;
            }

            // If one is null, but not both, return false.
            if (((object)a == null) || ((object)b == null))
            {
                return false;
            }

            // Return true if the fields match:
            return (a._filterValue == b._filterValue) &&
                (a._partition == b._partition) &&
                (a._companyPrefix == b._companyPrefix) &&
                (a._companyPrefixLengthInBits == b._companyPrefixLengthInBits) &&
                (a._companyPrefixLengthInDigits == b._companyPrefixLengthInDigits) &&
                (a._itemReference == b._itemReference) &&
                (a._itemReferenceLengthInBits == b._itemReferenceLengthInBits) &&
                (a._itemReferenceLengthInDigits == b._itemReferenceLengthInDigits) &&
                (a._serialNumber == b._serialNumber);
        }

        /// <summary>
        /// Compares two Sgtin96 object data member values.
        /// </summary>
        /// <param name="a">First Sgtin96 object to compare</param>
        /// <param name="b">Second Sgtin96 object to compare</param>
        /// <returns>
        /// false if data members of both objects match, true if they do not.
        /// </returns>
        public static bool operator !=(Sgtin96 a, Sgtin96 b)
        {
            return !(a == b);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode() ^ _filterValue;
        }

        #endregion

        #region private member functions

        private static bool IsValidUri(string CandidateSgtin)
        {
            bool ValidUriDetected = false;
            string[] DataValues;
            string SgtinCandidate = string.Empty;

            // Check to make sure that we have data to work with;
            // a null or empty string is invalid. Throw the appropriate
            // exception if this is the case.
            if (false == string.IsNullOrEmpty(CandidateSgtin))
            {
                SgtinCandidate = CandidateSgtin;

                // Strip out the fixed URI prefix string so that we can look at the data
                string DataFields = SgtinCandidate.Replace(_uriPrefix, string.Empty);
                // Now, extract each of the remaining fields as a separate string
                DataValues = DataFields.Split('.');

                // Define this variable here so that it has scope outside of
                // the IF. Negative logic used so that this value is meaningful
                // in determining the value of ValidUriDetected
                bool DataValuesAreNotNumeric = false;

                // If there are not enough URI fields, return false
                if (4 == DataValues.Length)
                {
                    // Ensure that each of the DataValues is numeric
                    Regex IsDigit = new Regex(@"[\d]+");
                    foreach (string str in DataValues)
                    {
                        if (false == IsDigit.IsMatch(str))
                        {
                            DataValuesAreNotNumeric = true;
                        }
                    }

                    // Do further sanity checks only if all DataValues are numeric
                    if (false == DataValuesAreNotNumeric)
                    {
                        // First, verify the Filter field
                        ushort FilterField = Convert.ToUInt16(DataValues[0], CONVERT_DECIMAL);

                        if (_filterValueMin <= FilterField
                            &&
                            _filterValueMax >= FilterField)
                        {
                            // Everything's good so far, so now check the company 
                            // prefix and item reference fields.  These must match
                            // one of the the following associations
                            //
                            // Company Prefix Digits | Item Reference Digits
                            // -----------------------------------------
                            //        12             |          1
                            //        11             |          2
                            //        10             |          3
                            //        9              |          4
                            //        8              |          5
                            //        7              |          6
                            //        6              |          7
                            // -----------------------------------------
                            // This table can be summarized by verifying that
                            // the number of Company Prefix digits is between 6 & 12
                            // and that the sum of the Company Prefix digits and
                            // Item Reference digits = 13.

                            string Prefix = DataValues[1];
                            int PrefixDigitCount = Prefix.Length;

                            string ItemReference = DataValues[2];
                            int ItemReferenceDigitCount = ItemReference.Length;

                            bool ValidCoPrefixAndItemRef = false;

                            if (6 <= PrefixDigitCount
                                &&
                                12 >= PrefixDigitCount
                                &&
                                (13 == (PrefixDigitCount + ItemReferenceDigitCount))
                                )
                            {
                                ValidCoPrefixAndItemRef = true;
                            }

                            // Now verify that the serial number is 38 bits or less
                            if (true == ValidCoPrefixAndItemRef)
                            {
                                string SerialNumber = DataValues[3];
                                UInt64 SerialNumberAsInt = Convert.ToUInt64(SerialNumber, CONVERT_DECIMAL);
                                int SerialNumberBitCount = 0;
                                string SerialNumberAsHex = SerialNumberAsInt.ToString("X10");
                                string SerialNumberAsBits = HexStringToBinString(SerialNumberAsHex).TrimStart('0');

                                SerialNumberBitCount = SerialNumberAsBits.Length;

                                if (_serialNumberMaxBits >= SerialNumberBitCount)
                                {
                                    ValidUriDetected = true;
                                }
                            }
                        }
                    }
                }
            }

            return ValidUriDetected;
        }

        private static bool IsValidEpc(string CandidateSgtin)
        {
            bool ValidEpcDetected = false;

            // Return false immediately if a Null or Empty string is passed in
            if (false == string.IsNullOrEmpty(CandidateSgtin))
            {
                // Verify that the string has the right number of characters
                // for an EPC
                if (24 == CandidateSgtin.Length)
                {
                    // Translate the EPC from a Hex string to a binary string
                    string BinaryEpc = HexStringToBinString(CandidateSgtin);
                    UInt16 HeaderValue;
                    UInt16 FilterValue;

                    // Verify that the EPC is represented by 96-bits
                    if (96 != BinaryEpc.Length)
                    {
                        throw new ArgumentException("Invalid SGTIN-96 EPC");
                    }
                    else
                    {
                        // Verify that the Header is correct
                        HeaderValue = Convert.ToUInt16(BinaryEpc.Substring(0, 8), CONVERT_BINARY);
                        if (_header == HeaderValue)
                        {
                            // Verify that the Filter is between the min and max values
                            FilterValue = Convert.ToUInt16(BinaryEpc.Substring(8, 3), CONVERT_BINARY);

                            if ((_filterValueMin <= FilterValue
                                &&
                                _filterValueMax >= FilterValue))
                            {
                                // Everything's good, so continue and verify the
                                // Partition is between min and max values
                                UInt16 Partition = Convert.ToUInt16(BinaryEpc.Substring(11, 3), 2);
                                if ((_partitionValueMin <= Partition
                                    &&
                                    _partitionValueMax >= Partition))
                                {
                                    // The remainder of the bits are very difficult
                                    // to verify, so call the EPC good
                                    ValidEpcDetected = true;
                                }
                            }
                        }
                    }
                }
            }

            return ValidEpcDetected;
        }

        private static void CalculateUpcCheckDigit(string UPC, out int checkDigit)
        {
            int check = 0;

            if (UPC == (new Regex("[^0-9]")).Replace(UPC, ""))
            {
                // pad with zeros to lengthen to 14 digits
                UPC = UPC.PadLeft(13, '0');

                // evaluate check digit
                int[] a = new int[13];
                a[0] = int.Parse(UPC[0].ToString()) * 3;
                a[1] = int.Parse(UPC[1].ToString());
                a[2] = int.Parse(UPC[2].ToString()) * 3;
                a[3] = int.Parse(UPC[3].ToString());
                a[4] = int.Parse(UPC[4].ToString()) * 3;
                a[5] = int.Parse(UPC[5].ToString());
                a[6] = int.Parse(UPC[6].ToString()) * 3;
                a[7] = int.Parse(UPC[7].ToString());
                a[8] = int.Parse(UPC[8].ToString()) * 3;
                a[9] = int.Parse(UPC[9].ToString());
                a[10] = int.Parse(UPC[10].ToString()) * 3;
                a[11] = int.Parse(UPC[11].ToString());
                a[12] = int.Parse(UPC[12].ToString()) * 3;
                int sum = a[0] + a[1] + a[2] + a[3] + a[4] + a[5] + a[6] + a[7] + a[8] + a[9] + a[10] + a[11] + a[12];
                check = (10 - (sum % 10)) % 10;
            }

            checkDigit = check;
        }

        public static bool IsValidGtin(string code)
        {
            if (code != (new Regex("[^0-9]")).Replace(code, ""))
            {
                // is not numeric
                return false;
            }
            // pad with zeros to lengthen to 14 digits
            switch (code.Length)
            {
                case 8:
                    code = "000000" + code;
                    break;
                case 12:
                    code = "00" + code;
                    break;
                case 13:
                    code = "0" + code;
                    break;
                case 14:
                    break;
                default:
                    // wrong number of digits
                    return false;
            }
            // calculate check digit
            int[] a = new int[13];
            a[0] = int.Parse(code[0].ToString()) * 3;
            a[1] = int.Parse(code[1].ToString());
            a[2] = int.Parse(code[2].ToString()) * 3;
            a[3] = int.Parse(code[3].ToString());
            a[4] = int.Parse(code[4].ToString()) * 3;
            a[5] = int.Parse(code[5].ToString());
            a[6] = int.Parse(code[6].ToString()) * 3;
            a[7] = int.Parse(code[7].ToString());
            a[8] = int.Parse(code[8].ToString()) * 3;
            a[9] = int.Parse(code[9].ToString());
            a[10] = int.Parse(code[10].ToString()) * 3;
            a[11] = int.Parse(code[11].ToString());
            a[12] = int.Parse(code[12].ToString()) * 3;
            int sum = a[0] + a[1] + a[2] + a[3] + a[4] + a[5] + a[6] + a[7] + a[8] + a[9] + a[10] + a[11] + a[12];
            int check = (10 - (sum % 10)) % 10;
            // evaluate check digit
            int last = int.Parse(code[13].ToString());
            return check == last;
        }

        public static bool IsValidUPC(string UPC)
        {
            if (UPC != (new Regex("[^0-9]")).Replace(UPC, ""))
            {
                // is not numeric
                return false;
            }

            // pad with zeros to lengthen to 14 digits
            switch (UPC.Length)
            {
                case 8:
                case 12:
                case 13:
                case 14:
                    UPC = UPC.PadLeft(14, '0');
                    break;
                default:
                    // wrong number of digits
                    return false;
            }

            // evaluate check digit
            int[] a = new int[13];
            a[0] = int.Parse(UPC[0].ToString()) * 3;
            a[1] = int.Parse(UPC[1].ToString());
            a[2] = int.Parse(UPC[2].ToString()) * 3;
            a[3] = int.Parse(UPC[3].ToString());
            a[4] = int.Parse(UPC[4].ToString()) * 3;
            a[5] = int.Parse(UPC[5].ToString());
            a[6] = int.Parse(UPC[6].ToString()) * 3;
            a[7] = int.Parse(UPC[7].ToString());
            a[8] = int.Parse(UPC[8].ToString()) * 3;
            a[9] = int.Parse(UPC[9].ToString());
            a[10] = int.Parse(UPC[10].ToString()) * 3;
            a[11] = int.Parse(UPC[11].ToString());
            a[12] = int.Parse(UPC[12].ToString()) * 3;
            int sum = a[0] + a[1] + a[2] + a[3] + a[4] + a[5] + a[6] + a[7] + a[8] + a[9] + a[10] + a[11] + a[12];
            int check = (10 - (sum % 10)) % 10;
            // evaluate check digit
            int last = int.Parse(UPC[13].ToString());
            return check == last;
        }

        private static void SetLengthsFromPartition(ref Sgtin96 Sgtin96ToUpdate)
        {
            switch (Sgtin96ToUpdate._partition)
            {
                case 0:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 40;
                    break;
                case 1:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 37;
                    break;
                case 2:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 34;
                    break;
                case 3:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 30;
                    break;
                case 4:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 27;
                    break;
                case 5:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 24;
                    break;
                case 6:
                    Sgtin96ToUpdate._companyPrefixLengthInBits = 20;
                    break;
                default:
                    break;
            }
            Sgtin96ToUpdate._itemReferenceLengthInBits =
                44 - Sgtin96ToUpdate._companyPrefixLengthInBits;
            Sgtin96ToUpdate._itemReferenceLengthInDigits =
                1 + Sgtin96ToUpdate._partition;
            Sgtin96ToUpdate._companyPrefixLengthInDigits =
                13 - Sgtin96ToUpdate._itemReferenceLengthInDigits;
        }

        #region Hex string to Bin string converter

        /// <summary>
        /// Utility that converts a valid, arbitrary length Hex string to its
        /// binary equivalent.
        /// </summary>
        /// <param name="InputString">
        /// Hexadecimal string to convert. Space delimited (e.g. "1111 2222")
        /// or dash delimited (e.g. "1111-2222") Hex strings allowed.
        /// </param>
        /// <returns>
        /// Binary string representation of the Hex string aligned on a 4-bit
        /// boundary, so an input of "7" will produce an output of "0111".
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when a non-Hex string is provided.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown when a null or empty Hex string is provided
        /// </exception>
        public static string HexStringToBinString(string InputString)
        {
            StringBuilder ReturnString = new StringBuilder();
            StringBuilder SourceString = new StringBuilder();

            if (false == string.IsNullOrEmpty(InputString))
            {
                // First, append InputString to SourceString
                SourceString.Append(InputString);

                // Next, remove any space or dash delimiters
                SourceString.Replace(" ", string.Empty);
                SourceString.Replace("-", string.Empty);

                // Now, verify that the source string contains only hex characters
                if (IsHexString(SourceString.ToString()))
                {
                    // We have a valid hex string, so iterate through the string
                    // one character at a time, appending the binary value for
                    // each char to ReturnString as we go
                    for (int i = 0; i < SourceString.Length; i++)
                    {
                        // Extract the current 4 chars
                        string CurrentChar = SourceString.ToString().Substring(i, 1);
                        // Convert this from a Hex to decimal Int32
                        Int32 CurrentCharAsInt = Convert.ToInt32(CurrentChar, 16);
                        // Append the binary representation of the current 4 chars
                        ReturnString.Append(Convert.ToString(CurrentCharAsInt, 2).PadLeft(4, '0'));
                    }
                }
                else
                {
                    throw new ArgumentException("Provided string not in hexadecimal format");
                }
            }
            else
            {
                throw new ArgumentNullException();
            }

            return ReturnString.ToString();
        }

        #endregion

        #region Bin String to Hex String converter

        /// <summary>
        /// Utility that converts a valid, arbitrary length binary string to its
        /// Hex equivalent.
        /// </summary>
        /// <param name="InputString">
        /// Binary string to convert. Space delimited (e.g. "1111 0000")
        /// or dash delimited (e.g. "1111-0000") binary strings allowed.
        /// </param>
        /// <returns>
        /// Left zero-padded Hexadecimal string representation of the binary
        /// string aligned on a 4-bit boundary, so an input of "00111" will
        /// produce an output of "07".
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when a non-binary string is provided.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown when a null or empty binary string is provided
        /// </exception>
        public static string BinStringToHexString(string InputString)
        {
            StringBuilder ReturnString = new StringBuilder();
            StringBuilder SourceString = new StringBuilder();

            if (false == String.IsNullOrEmpty(InputString))
            {
                // We need to process the binary information in sets of 4 bits,
                // with any string that is not a multiple of 4 being zero
                // padded in the MSBs. Determine how many bits there are,
                // and pad left with the appropriate number of zeros

                // First, assign the InputString to the SourceString work
                // object.
                SourceString.Append(InputString);

                // Next, remove any space or dash delimiters
                SourceString.Replace(" ", string.Empty);
                SourceString.Replace("-", string.Empty);

                // If the binary string is already in multiples of 4 bits,
                // then there is nothing to do
                if (0 != SourceString.Length % 4)
                {
                    // Add zeros until we hit the next 4-bit boundary
                    for (int i = 0; 0 != (SourceString.Length % 4); i++)
                    {
                        SourceString.Insert(0, '0');
                    }
                }

                if (OnlyBinInString(SourceString.ToString()))
                {
                    string SourceSubString = string.Empty;

                    for (int i = 0; i <= (SourceString.Length - 4); i += 4)
                    {
                        SourceSubString = SourceString.ToString().Substring(i, 4);
                        ReturnString.Append(string.Format("{0:X}", Convert.ToByte(SourceSubString, 2)));
                    }
                }
                else
                {
                    throw new ArgumentException("Provided string not in binary format");
                }
            }
            else
            {
                throw new ArgumentNullException();
            }

            return ReturnString.ToString();
        }
        #endregion

        #region Utilities for testing whether a string contains only hex or binary characters

        /// <summary>
        /// Utility for verifying that a given string includes only Hexadecimal
        /// characters.
        /// </summary>
        /// <param name="InputString">
        /// A hexadecimal string to test in capitals or lower case, either with
        /// the "0x" prefix, or without.
        /// </param>
        /// <returns>
        /// true if the string contains only Hexadecimal characters.
        /// false if any non-hex characters are detected.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown if a null or empty string is passed in.
        /// </exception>
        public static bool IsHexString(string InputString)
        {
            bool returnResult = false;
            StringBuilder SourceString = new StringBuilder();

            if (false == String.IsNullOrEmpty(InputString))
            {
                // Append the input string to the SourceString work variable
                SourceString.Append(InputString);

                // Remove any prefix characters
                SourceString.Replace("0x", string.Empty);

                // Next, remove any space or dash delimiters
                SourceString.Replace(" ", string.Empty);
                SourceString.Replace("-", string.Empty);

                returnResult =
                    System.Text.RegularExpressions.
                    Regex.IsMatch(SourceString.ToString(), @"\A\b[0-9a-fA-F]+\b\Z");
            }
            else
            {
                throw new ArgumentNullException();
            }

            return returnResult;
        }

        /// <summary>
        /// Utility for verifying that a given string includes only decimal
        /// characters.
        /// </summary>
        /// <param name="InputString">
        /// A string to test
        /// </param>
        /// <returns>
        /// true if the string contains only decimal characters.
        /// false if any non-decimal characters are detected.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown if a null or empty string is passed in.
        /// </exception>
        public static bool IsDecimalString(string InputString)
        {
            bool returnResult = false;
            StringBuilder SourceString = new StringBuilder();

            if (false == String.IsNullOrEmpty(InputString))
            {
                // Append the input string to the SourceString work variable
                SourceString.Append(InputString);

                // Remove any prefix characters
                SourceString.Replace("0x", string.Empty);

                // Next, remove any space or dash delimiters
                SourceString.Replace(" ", string.Empty);
                SourceString.Replace("-", string.Empty);

                returnResult =
                    System.Text.RegularExpressions.
                    Regex.IsMatch(SourceString.ToString(), @"\A\b[\d]+\b\Z");
            }
            else
            {
                throw new ArgumentNullException();
            }

            return returnResult;
        }

        /// <summary>
        /// Utility for verifying that a given string includes only binary
        /// characters.
        /// </summary>
        /// <param name="InputString">
        /// A string of characters to test.
        /// </param>
        /// <returns>
        /// true if the string contains only decimal characters.
        /// false if any non-decimal characters are detected.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown if a null or empty string is passed in.
        /// </exception>
        public static bool OnlyBinInString(string InputString)
        {
            bool returnResult = false;
            StringBuilder SourceString = new StringBuilder();

            if (false == String.IsNullOrEmpty(InputString))
            {
                SourceString.Append(InputString);

                // Next, remove any space or dash delimiters
                SourceString.Replace(" ", string.Empty);
                SourceString.Replace("-", string.Empty);

                returnResult =
                    System.Text.RegularExpressions.
                    Regex.IsMatch(SourceString.ToString(), @"\A\b[01]+\b\Z");
            }
            else
            {
                throw new ArgumentNullException();
            }

            return returnResult;
        }

        #endregion
    }
    #endregion
}