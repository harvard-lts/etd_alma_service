# Using the METS -> MARC commandline tool

This tool is designed to be run as a commandline Docker service.
The Docker image can either be built (details below) or pulled from the
HUIT Artifactory repository:
  - published at: artifactory.huit.harvard.edu/lts/etd_alma_service_cli

## How to build the local Docker image
1. Create or navigate to a local directory where you will clone this repository
1. Clone this repository
   ```
   git clone https://github.com/harvard-lts/etd_alma_service.git
   ```
1. Navigate into the local clone directory
   ```
   cd etd_alma_service
   ```
1. Switch to the `cli` Git branch
   ```
   git checkout cli
   ```
1. Build image
   ```
   docker build . -t etd_alma_service_cli:latest -f DockerfileCLI
   ```
1. Verify successful image creation; show help menu
   ```
   docker run --rm --mount type=bind,source=${PWD},target=/work -it etd_alma_service_cli -h
   ```
   1. Expected output
      ```
      usage: main.py [-h] -i INPUT_METS -o OUTPUT_DIR -f OUTPUT_FILE [-v | --verbose | --no-verbose]

      Converts a ProQuest METS XML file to MARC XML

      options:
        -h, --help            show this help message and exit
        -i INPUT_METS, --input_mets INPUT_METS
                              Local METS XML file to be converted to MARC XML
        -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                              Local directory where MARC XML will be written; Note, this directory must exist before running tool
        -f OUTPUT_FILE, --output_file OUTPUT_FILE
                              Filename of MARC XML that will be created in the output_dir; Note, the extension ".xml"
                              will be added to the provided output filename
        -v, --verbose, --no-verbose
                              Flag used to show debug message when running tool, default=True
      ```

## How to run the tool
1. Run tool as mentioned in the last step above, providing input METS XML and output info:
   ```
   docker run --rm --mount type=bind,source=${PWD},target=/work -it etd_alma_service_cli -i <local-path/a-mets.xml> -o <local-output-dir> -f <filename-for-new-marc>
   ```
1. Example:
   ```
   docker run --rm --mount type=bind,source=${PWD},target=/work -it etd_alma_service_cli -i input/mets_1.xml -o output -f marc_1
   ```
