# ICEES-KG Overview

This tool generates p-values and chi-squared values between curies. These p-values are generated by looking at the correlations between features in the patient/feature ICEES database.

These scripts were tested and assumptions were written for macOSX and python3.9, but should work on Ubuntu and other versions of python. If an older version of python is to be used, it's likely that the version numbers of some of the packages need to be downgraded in the ./requirements.txt file.

This should work with windows as well, but file paths will need to be modified.

# Getting Started
This section will describe the steps that need to be performed before any of the p-values are computed or jsonl files created.

## External Dependencies
This tool has an external dependency:
- [ORION](https://github.com/RobokopU24/ORION)

The ORION repo is used to help generate jsonl files that define the nodes and edges of a graph. In this graph, the curies are the nodes, and the edges link all the nodes and include a "p_value" property. These jsonl files are then ingested into [PLATER](https://github.com/TranslatorSRI/Plater) and served up by [AUTOMAT](https://github.com/RENCI-AUTOMAT/Automat-server).

We basically just need a few helper functions from ORION, so we only need a specific folder from the ORION repo. How we're currently do this is by cloning down the repo and then copying over the `Common` folder into `./icees-kg` and put it beside `main.py`.

## Create a Virtual Python Environment

First, create, activate, and update a virutal environment.

```bash
python3.9 -m venv <path_to_venv>
source <path_to_venv>/bin/activate
pip install --upgrade pip
```
Usually <path_to_venv> is set to ~/.venvs/<venv_name>

Next, install all requirements needed to run p-value scripts.

```bash
pip install -r requirements.txt
```

## Create .env file
Create a .env file in the root directory and add the following variables:

DATASET_NAME="FILL_THIS_IN" # asthma, dili, pcd, covid, etc.\
DATA_PATH="FILL_THIS_IN"\
FEATURES_YAML="FILL_THIS_IN"\
NODE_NORM="FILL_THIS_IN"\
NAME_RESOLVER="FILL_THIS_IN"\

# Generate P-Values jsonl files
First, environment variables need to be set prior to running the script. (My goto bash command is `export $(grep -v '^#' .env | xargs)` to load the env in.)
Then run `icees_kg/main.py`. That will output both node and edge files into a top-level `./build` folder.

_EDIT:_ This will output temporal edges that do not get ingested into ORION super well. There is an extra script that you now need to run to massage the files some.\
Once you have the nodes.jsonl and edges.jsonl files in your `./build` folder, then run `icees_kg/massage.py`.\
Take those two jsonl files and go on to the next step!


# Take jsonl files and upload to renci server so SRI team can deploy
- Log in to hop.renci.org server
- Upload dump file to /projects/stars/var/plater/bl-2.1/
- Update helm charts in translator-devops repo and install
