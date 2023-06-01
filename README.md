# data_adapter_oemof

A data adapter to create datapackages for oemof.tabular/oemof.solph from the SEDOS input data model.

## Getting started

To install data_adapter_oemof, follow these steps:

* git-clone data_adapter_oemof into local folder:
  `git clone https://github.com/rl-institut/data_adapter_oemof.git
* enter folder `cd data_adapter_oemof`
* create virtual environment using conda: `conda env create -f environment.yml`
* activate environment: `conda activate data_adapter_oemof`
* install data_adapter_oemof package using poetry, via: `poetry install`

## Docs

To build the docs simply go to the `docs` folder

    cd docs

Install the requirements

    pip install -r docs_requirements.txt

and run

    make html

The output will then be located in `docs/_build/html` and can be opened with your favorite browser

## Code linting

In this template, 3 possible linters are proposed:
- flake8 only sends warnings and error about linting (PEP8)
- pylint sends warnings and error about linting (PEP8) and also allows warning about imports order
- black sends warning but can also fix the files for you

You can perfectly use the 3 of them or subset, at your preference. Don't forget to edit `.travis.yml` if you want to deactivate the automatic testing of some linters!
