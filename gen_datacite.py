#!/usr/bin/env python

import json
from collections import OrderedDict

# Defined in:
# https://schema.datacite.org/meta/kernel-4.1/doc/DataCite-MetadataKernel_v4.1.pdf # noqa

DATACITE_TITLE_TYPES = ['AlternativeTitle', 'Subtitle', 'TranslatedTitle',
                        'Other']

DATACITE_RESOURCE_TYPES = ['Audiovisual', 'Collection', 'DataPaper', 'Dataset',
                           'Event', 'Image', 'InteractiveResource', 'Model',
                           'PhysicalObject', 'Service', 'Software', 'Sound',
                           'Text', 'Workflow', 'Other']

DATACITE_CONTRIBUTOR_TYPES = ['ContactPerson', 'DataCollector', 'DataCurator',
                              'DataManager', 'Distributor', 'Editor',
                              'HostingInstitution', 'Producer',
                              'ProjectLeader', 'ProjectManager',
                              'ProjectMember', 'RegistrationAgency',
                              'RegistrationAuthority', 'RelatedPerson',
                              'Researcher', 'ResearchGroup', 'RightsHolder',
                              'Sponsor', 'Supervisor', 'WorkPackageLeader',
                              'Other']

DATACITE_DATE_TYPES = ['Accepted', 'Available', 'Copyrighted', 'Collected',
                       'Created', 'Issued', 'Submitted', 'Updated', 'Valid',
                       'Other']

DATACITE_RELATED_IDENTIFIER_TYPES = [
    'ARK', 'arXiv', 'bibcode', 'DOI', 'EAN13', 'EISSN', 'Handle', 'IGSN',
    'ISBN', 'ISSN', 'ISTC', 'LISSN', 'LSID', 'PMID', 'PURL', 'UPC', 'URL',
    'URN']

DATACITE_RELATED_IDENTIFIER_RELATION_TYPES = [
    'IsCitedBy', 'Cites', 'IsSupplementTo', 'IsSupplementedBy',
    'IsContinuedBy', 'Continues', 'IsDescribed by', 'Describes', 'HasMetadata',
    'IsMetadataFor', 'HasVersion', 'IsVersionOf', 'IsNewVersionOf',
    'IsPreviousVersionOf', 'IsPartOf', 'HasPart', 'IsReferencedBy',
    'References', 'IsDocumentedBy', 'Documents', 'IsCompiledBy', 'Compiles',
    'IsVariantFormOf', 'IsOriginalFormOf', 'IsIdenticalTo', 'IsReviewedBy',
    'Reviews', 'IsDerivedFrom', 'IsSourceOf', 'IsRequiredBy', 'Requires',
]

DATACITE_DESCRIPTION_TYPES = ['Abstract', 'Methods', 'SeriesInformation',
                              'TableOfContents', 'TechnicalInfo', 'Other']


# Based on the Datacite 4.1 Schema shown here:
# https://schema.datacite.org/meta/kernel-4.1/doc/DataCite-MetadataKernel_v4.1.pdf  # noqa
FIELD_GENERATORS = (
    # Occurrences identifier: 1
    ('identifier', lambda x: {
        'identifier_type': 'DOI',
        'value': ''
    }),
    # Occurrences creator: 1-n
    ('creators', lambda x: [{
            # Occurrences: 1
            'creator_name': {
                'value': name,
                # Occurrences: 0-1
                # Must be one of Organisational, Personal
                'name_type': 'Personal'
            },
            # # Occurrences: 0-1
            # 'given_name': 'name',
            # # Occurrences: 0-1
            # 'family_name': 'name',
            # # Occurrences: 0-n
            # 'name_identifier': [{
            #     # Occurrences: 1
            #     'name_identifier_scheme': 'foo',
            #     'scheme_uri': 'bar'
            # }],
            # # Occurrences: 0-1
            # 'affiliation': 'name',
        } for name in ['Seo J', 'Ju YS', 'Lee W']
    ]),
    # Occurrences: 0-n
    ('subjects', lambda x: [
        {'value': 	'Homo sapiens'}
    ]),
    # Occurrences: 1-n
    ('titles', lambda x: [{
        'value': 'The transcriptional landscape and mutational '
                 'profile of lung adenocarcinoma',
        # Occurrences: 0-n
        'title_type': 'Subtitle',
    }]),
    # Occurrences: 1
    ('publisher', lambda x: {
        'value': 'MD Anderson Cancer Center',
    }),
    # Occurrences: 1
    ('publication_year', lambda x: {
        'value': '2012',
    }),
    # Occurrences: 1
    ('resource_type', lambda x: {
        # Occurrences: 1
        'value': 'Dataset/GSM Samples',
        # Occurrences: 1
        'resource_type_general': 'Dataset',
    }),
    # Occurrences: 0-n
    # ('contributors', lambda x: [
    #     {
    #         # Occurrences: 1
    #         'contributor_type': 'Other',
    #         # Occurrences: 1
    #         'contributor_name': name,
    #         # # Occurrences: 0-1
    #         # 'nameType': 'foo',
    #         # # Occurrences: 0-1
    #         # 'familyName': 'foo',
    #         # # Occurrences: 0-1
    #         # 'given_name': 'foo',
    #         # # Occurrences: 0-1
    #         # 'name_identifier': {
    #         #     # Occurrences: 1
    #         #     'name_identifier_scheme': 'foo',
    #         #     # Occurrences: 0-1
    #         #     'scheme_uri': 'bar'
    #         # },
    #         # # Occurrences: 0-n Free Text
    #         # 'affiliation': [],
    #     } for name in ['Seo J', 'Ju YS', 'Lee W']
    # ]),
    # Occurrences: 0-n
    ('dates', lambda x: [
            {
                'date_type': 'Submitted',
                'value': '2012-08-28'
            },
            {
                'date_type': 'Updated',
                'value': '2018-06-11'
            },
            {
                'date_type': 'Public',
                'value': '2012-09-06'
            }
        ]
     ),
    # Occurrences: 0-1
    ('language', lambda x: {
        'value': 'en'
    }),
    # Occurrences: 0-n
    # ('alternate_identifiers', lambda x: [{
    # Occurrences: 1
    # 'alternateIdentifierType': 'foo',
    # 'value': 'foo'
    # } for n in range(NUM_ALTERNATE_IDENTIFIERS)),
    # Occurrences: 0-n
    # ('related_identifiers', lambda x: [{
    #     # Occurrences: 1
    #     'related_identifier_type': 'DOI',
    #     # 'related_identifier_type':
    # ranlist(DATACITE_RELATED_IDENTIFIER_TYPES)
    #     # Occurances: 1
    #     'relation_type': ranlist(DATACITE_RELATED_IDENTIFIER_RELATION_TYPES),
    #     'value': '{}.{}/foo-bar-doi-1.0'.format(
    #         random.randint(1, 10),
    #         random.randint(1000, 9000)
    #     )
    #     # # Occurrences: 0-1
    #     # 'relatedMetadataScheme': '',
    #     # # Occurrences: 0-1
    #     # 'schemeURI': '',
    #     # # Occurrences: 0-1
    #     # 'schemeType': '',
    # } for n in range(NUM_RELATED_IDENTIFIERS)]),
    # Occurrences: 0-n
    # ('sizes', lambda x: [{
    #         'value': '{} {}'.format(
    #             random.randint(1, 1000),
    #             ranlist(DEF_SIZE_TYPES)
    #         )
    #     } for n in range(NUM_SIZES)
    # ]),
    # Occurrences: 0-n
    ('formats', lambda x: [
        'SOFT', 'MINiML', 'TXT'
     ]),
    # Occurrences: 0-1
    # ('version', lambda x: {
    #     'value': '1.0.0'
    # }),
    # Occurrences: 0-n
    # ('rights', lambda x: {
    #     'value': ranlist(['https://opensource.org/licenses/GPL-3.0']),
    #     # Occurrences: 0-1
    #     'rights_uri': ranlist(['https://opensource.org/licenses/GPL-3.0'])
    # }),
    # Occurrences: 0-n
    ('descriptions', lambda x: {
        'value': 'Understanding the molecular signatures of cancer is '
                 'important to apply appropriate targeted therapies. Here we '
                 'present the first large scale RNA sequencing study of lung '
                 'adenocarcinoma demonstrating its power to identify somatic '
                 'point mutations as well as transcriptional variants such as '
                 'gene fusions, alternative splicing events and expression '
                 'outliers. Our results reveal the genetic basis of 200 lung '
                 'adenocarcinomas in Koreans including deep characterization '
                 'of 87 surgical specimens by transcriptome sequencing. We '
                 'identified driver somatic mutations in cancer genes '
                 'including EGFR, KRAS, NRAS, BRAF, PIK3CA, MET and CTNNB1. '
                 'New cancer genes, such as LMTK2, ARID1A, NOTCH2 and '
                 'SMARCA4, were also suggested as candidates for novel '
                 'drivers in lung adenocarcinoma. We found 45 fusion genes, '
                 '8 of which were chimeric tyrosine kinases involving ALK, '
                 'RET, ROS1, FGFR2, AXL and PDGFRA. Of 17 recurrent '
                 'alternative splicing events, we identified exon 14 skipping '
                 'in the proto-oncogene MET as highly likely to be a cancer '
                 'driver. The number of somatic mutations and expression '
                 'outliers varied markedly between individual cancers and '
                 'was strongly correlated with smoking history of cancer '
                 'patients. In addition, we identified genomic blocks where '
                 'genes were frequently up- or down-regulated together that '
                 'could be explained by copy number alterations in the cancer '
                 'tissue. We also found an association between lymph node '
                 'metastasis and somatic mutations in TP53. Our findings '
                 'broaden our understanding of lung adenocarcinoma and may '
                 'also lead to new diagnostic and therapeutic approaches.',
        # We'll only use Abstract for this mock data for now
        'description_type': 'Abstract',
        # 'description_type': ranlist(DATACITE_DESCRIPTION_TYPES)
    }),
    # ('geo_location', lambda x: {
    # 'value': ''
    # }),

)


def gen_datacite_entry():
    """Generate a Datacite Search entry."""
    search_entry = OrderedDict()
    for name, func in FIELD_GENERATORS:
        search_entry[name] = func(search_entry)
    return search_entry


if __name__ == '__main__':
    """Pretty print generated data to console"""
    print(json.dumps(gen_datacite_entry(), indent=4))
