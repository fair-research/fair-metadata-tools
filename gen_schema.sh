#!/bin/bash

declare -a ids=(
"SAMPID"
"SUBJID"
"EBI-Accession"
"Forward_path"
"Reverse_path"
"SEX"
"AGE"
"SMOKING_STATUS"
"STAGE"
"SMATSSCR"
"SMCENTER"
"SMPTHNTS"
"SMRIN"
"SMTS"
"SMTSD"
"SMUBRID"
"SMTSISCH"
"SMTSPAX"
"SMNABTCH"
"SMNABTCHT"
"SMNABTCHD"
"SMGEBTCH"
"SMGEBTCHD"
"SMGEBTCHT"
"LIBRARY_TYPE"
"SME2MPRT"
"SMCHMPRS"
"SMNTRART"
"SMNUMGPS"
"SMMAPRT"
"SMEXNCRT"
"SM550NRM"
"SMGNSDTC"
"SMUNMPRT"
"SM350NRM"
"SMRDLGTH"
"SMMNCPB"
"SME1MMRT"
"SMSFLGTH"
"SMESTLBS"
"SMMPPD"
"SMNTERRT"
"SMRRNANM"
"SMRDTTL"
"SMVQCFL"
"SMMNCV"
"SMTRSCPT"
"SMMPPDPR"
"SMCGLGTH"
"SMGAPPCT"
"SMUNPDRD"
"SMNTRNRT"
"SMMPUNRT"
"SMEXPEFF"
"SMMPPDUN"
"SME2MMRT"
"SME2ANTI"
"SMALTALG"
"SME2SNSE"
"SMMFLGTH"
"SME1ANTI"
"SMSPLTRD"
"SMBSMMRT"
"SME1SNSE"
"SME1PCTS"
"SMRRNART"
"SME1MPRT"
"SMNUM5CD"
"SMDPMPRT"
"SME2PCTS"
)

cat <<EOF
<?xml version="1.0"?>
<xs:schema targetNamespace="http://gtex.globuscs.info/meta/GTEx_v7.xsd" xmlns:xs="http://www.w3.org/2001/XMLSchema" xml:lang="EN">
  <xs:element name="content">
    <xs:complexType>
      <xs:sequence>
EOF

for i in "${ids[@]}"; do
    echo "        <xs:element name=\"$i\" type=\"xs:string\"/>"
done

echo '        <xs:element name="description">'
echo '          <xs:complexType>'
echo '            <xs:sequence>'

for i in "${ids[@]}"; do
    echo "              <xs:element name=\"$i\" type=\"xs:string\"/>"
done

echo '            </xs:sequence>'
echo '          </xs:complexType>'
echo '        </xs:element>'
echo '      </xs:sequence>'
echo '    </xs:complexType>'
echo '  </xs:element>'
echo '</xs:schema>'
