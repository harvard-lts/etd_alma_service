import argparse
from etd.worker import writeMarcXml
from etd.worker import getFromMets

##
# Main ##
##

if __name__ == '__main__':
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser(description="Converts a ProQuest METS XML "
                                             "file to MARC XML")

    ap.add_argument('-i', '--input_mets',
                    required=True,
                    help='Local METS XML file to be converted to MARC XML')
    ap.add_argument('-o', '--output_dir',
                    required=True,
                    help='Local directory where MARC XML will be written; '
                    'Note, this directory must exist before running tool')
    ap.add_argument('-f', '--output_file',
                    required=True,
                    help='Filename of MARC XML that will be created in the '
                         'output_dir; Note, the extension ".xml" '
                         'will be added to the provided output filename')
    ap.add_argument('-v', '--verbose',
                    required=False, default=True,
                    action=argparse.BooleanOptionalAction,
                    help='Flag used to show debug message when running tool; '
                          'default=True')
    args = vars(ap.parse_args())

    input_mets = args['input_mets']
    output_dir = args['output_dir']
    output_file = args['output_file']
    verbose = args['verbose']

    batchOutputDir = output_dir
    batch = output_file

    generatedMarcXmlValues = getFromMets(input_mets, verbose)
    if generatedMarcXmlValues:
        writeMarcXml(batch, batchOutputDir, generatedMarcXmlValues, verbose)
    else:
        print('Error getting all necessary fields from {}'.format(input_mets))
