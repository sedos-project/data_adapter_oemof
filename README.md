# data_adapter_oemof

A data adapter to create datapackages for oemof.tabular from the output of the general [data_adapter](https://github.com/sedos-project/data_adapter). 

## Getting Started

### Installation

To install data_adapter_oemof, follow these steps:

* git-clone data_adapter_oemof into local folder:
  `git clone https://github.com/rl-institut/data_adapter_oemof.git
* enter folder `cd data_adapter_oemof`
* create virtual environment using conda: `conda env create -f environment.yml`
* activate environment: `conda activate data_adapter_oemof`
* install data_adapter_oemof package using poetry, via: `poetry install`

### Functionalities
The data_adapter_oemof comprises three primary functionalities:

1. Mappings: The mappings include translation tables for parameters, enabling the program to appropriately assign the provided data to the parameters defined in oemof.tabular.

2. Adapter: The adapter encompasses the facades, which serve as crucial templates for the creation of components within the energy system. It also includes various extensions and additional specifications relevant to the facades.

3. Functions for Data Compatibility: A set of functions is available within the oemof.tabular builder to facilitate the generation of compatible data packages. These packages can be produced from data sourced from the public data bus. For this process to occur, the data records must be formatted in accordance with the parameter model data structure, and appropriate structure files and mappings must be supplied. The builder interacts with both the mapper and the adapter to accomplish this task.

## Code linting

In this template, 3 possible linters are proposed:
- flake8 only sends warnings and error about linting (PEP8)
- pylint sends warnings and error about linting (PEP8) and also allows warning about imports order
- black sends warning but can also fix the files for you

You can perfectly use the 3 of them or subset, at your preference. Don't forget to edit `.travis.yml` if you want to deactivate the automatic testing of some linters!
