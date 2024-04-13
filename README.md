# pat2vec
Converts individual patient data into time interval feature vectors, suitable for filtering and concatenation into a data matrix D for binary classification machine learning tasks.


Example use case 1: I aim to compute the mean of n variables for each unique patient, resulting in a single row representing each patient.

Example use case 2: I intend to generate a monthly time series comprising patient data encompassing biochemistry, demographic details, and textual annotations (MedCat annotations) spanning the last 25 years. Each patient's data begins from a distinct start date (diagnosis date), providing a retrospective view.



## Table of Contents
- [Requirements](#requirements)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Requirements:

- CogStack (cogstack_v8_lite)
- Elasticsearch
- MedCat

## Dependencies:

- [accelerate](https://pypi.org/project/accelerate/)==0.24.1
- [aiofiles](https://pypi.org/project/aiofiles/)==23.2.1
- [aiohttp](https://pypi.org/project/aiohttp/)==3.8.5
- [aiosignal](https://pypi.org/project/aiosignal/)==1.3.1
- [annotated-types](https://pypi.org/project/annotated-types/)==0.6.0
- [anyio](https://pypi.org/project/anyio/)==4.0.0
- [argon2-cffi](https://pypi.org/project/argon2-cffi/)==23.1.0
- [argon2-cffi-bindings](https://pypi.org/project/argon2-cffi-bindings/)==21.2.0
- [arrow](https://pypi.org/project/arrow/)==1.3.0
- [astor](https://pypi.org/project/astor/)==0.8.1
- [asttokens](https://pypi.org/project/asttokens/)==2.4.1
- [async-lru](https://pypi.org/project/async-lru/)==2.0.4
- [async-timeout](https://pypi.org/project/async-timeout/)==4.0.3
- [attrs](https://pypi.org/project/attrs/)==23.1.0
- [autograd](https://pypi.org/project/autograd/)==1.6.2
- [autograd-gamma](https://pypi.org/project/autograd-gamma/)==0.5.0
- [Babel](https://pypi.org/project/Babel/)==2.13.1
- [bcrypt](https://pypi.org/project/bcrypt/)==4.0.1
- [beautifulsoup4](https://pypi.org/project/beautifulsoup4/)==4.12.2
- [bleach](https://pypi.org/project/bleach/)==6.1.0
- [blis](https://pypi.org/project/blis/)==0.7.11
- [cachetools](https://pypi.org/project/cachetools/)==4.2.4
- [catalogue](https://pypi.org/project/catalogue/)==2.0.10
- [certifi](https://pypi.org/project/certifi/)==2023.7.22
- [cffi](https://pypi.org/project/cffi/)==1.16.0
- [charset-normalizer](https://pypi.org/project/charset-normalizer/)==3.3.2
- [click](https://pypi.org/project/click/)==8.1.7
- [cloudpathlib](https://pypi.org/project/cloudpathlib/)==0.16.0
- [colorama](https://pypi.org/project/colorama/)==0.4.6
- [comm](https://pypi.org/project/comm/)==0.1.4
- [confection](https://pypi.org/project/confection/)==0.1.3
- [contourpy](https://pypi.org/project/contourpy/)==1.1.1
- [cryptography](https://pypi.org/project/cryptography/)==41.0.5
- [cycler](https://pypi.org/project/cycler/)==0.12.1
- [cymem](https://pypi.org/project/cymem/)==2.0.8
- [dacite](https://pypi.org/project/dacite/)==1.8.1
- [datasets](https://pypi.org/project/datasets/)==2.14.6
- [DateTime](https://pypi.org/project/DateTime/)==5.3
- [debugpy](https://pypi.org/project/debugpy/)==1.8.0
- [decorator](https://pypi.org/project/decorator/)==5.1.1
- [defusedxml](https://pypi.org/project/defusedxml/)==0.7.1
- [dill](https://pypi.org/project/dill/)==0.3.7
- [eland](https://pypi.org/project/eland/)==8.10.1
- [elastic-transport](https://pypi.org/project/elastic-transport/)==8.10.0
- [elasticsearch](https://pypi.org/project/elasticsearch/)==8.10.1
- [et-xmlfile](https://pypi.org/project/et-xmlfile/)==1.1.0
- [exceptiongroup](https://pypi.org/project/exceptiongroup/)==1.1.3
- [executing](https://pypi.org/project/executing/)==2.0.1
- [Faker](https://pypi.org/project/Faker/)==24.9.0
- [fastjsonschema](https://pypi.org/project/fastjsonschema/)==2.18.1
- [filelock](https://pypi.org/project/filelock/)==3.13.1
- [fitter](https://pypi.org/project/fitter/)==1.6.0
- [fonttools](https://pypi.org/project/fonttools/)==4.44.0
- [formulaic](https://pypi.org/project/formulaic/)==0.6.6
- [fqdn](https://pypi.org/project/fqdn/)==1.5.1
- [frozenlist](https://pypi.org/project/frozenlist/)==1.4.0
- [fsspec](https://pypi.org/project/fsspec/)==2023.10.0
- [future](https://pypi.org/project/future/)==0.18.3
- [fuzzywuzzy](https://pypi.org/project/fuzzywuzzy/)==0.18.0
- [gensim](https://pypi.org/project/gensim/)==4.3.2
- [h11](https://pypi.org/project/h11/)==0.14.0
- [htmlmin](https://pypi.org/project/htmlmin/)==0.1.12
- [httpcore](https://pypi.org/project/httpcore/)==1.0.5
- [httpx](https://pypi.org/project/httpx/)==0.27.0
- [huggingface-hub](https://pypi.org/project/huggingface-hub/)==0.21.4
- [idna](https://pypi.org/project/idna/)==3.4
- [ImageHash](https://pypi.org/project/ImageHash/)==4.3.1
- [importlib-metadata](https://pypi.org/project/importlib-metadata/)==6.8.0
- [importlib-resources](https://pypi.org/project/importlib-resources/)==6.1.0
- [interface-meta](https://pypi.org/project/interface-meta/)==1.3.0
- [ipykernel](https://pypi.org/project/ipykernel/)==6.26.0
- [ipython](https://pypi.org/project/ipython/)==8.17.2
- [ipython-genutils](https://pypi.org/project/ipython-genutils/)==0.2.0
- [ipywidgets](https://pypi.org/project/ipywidgets/)==8.1.1
- [isoduration](https://pypi.org/project/isoduration/)==20.11.0
- [jedi](https://pypi.org/project/jedi/)==0.19.1
- [Jinja2](https://pypi.org/project/Jinja2/)==3.1.2
- [joblib](https://pypi.org/project/joblib/)==1.3.2
- [json5](https://pypi.org/project/json5/)==0.9.14
- [jsonpickle](https://pypi.org/project/jsonpickle/)==3.0.2
- [jsonpointer](https://pypi.org/project/jsonpointer/)==2.4
- [jsonschema](https://pypi.org/project/jsonschema/)==4.19.2
- [jsonschema-specifications](https://pypi.org/project/jsonschema-specifications/)==2023.7.1
- [jupyter](https://pypi.org/project/jupyter/)==1.0.0
- [jupyter-console](https://pypi.org/project/jupyter-console/)==6.6.3
- [jupyter-events](https://pypi.org/project/jupyter-events/)==0.8.0
- [jupyter-lsp](https://pypi.org/project/jupyter-lsp/)==2.2.0
- [jupyter_client](https://pypi.org/project/jupyter-client/)==8.5.0
- [jupyter_core](https://pypi.org/project/jupyter-core/)==5.5.0
- [jupyter_server](https://pypi.org/project/jupyter-server/)==2.9.1
- [jupyter_server_terminals](https://pypi.org/project/jupyter-server-terminals/)==0.4.4
- [jupyterlab](https://pypi.org/project/jupyterlab/)==4.0.8
- [jupyterlab-pygments](https://pypi.org/project/jupyterlab-pygments/)==0.2.2
- [jupyterlab_server](https://pypi.org/project/jupyterlab-server/)==2.25.0
- [jupyterlab_widgets](https://pypi.org/project/jupyterlab-widgets/)==3.0.10
- [kiwisolver](https://pypi.org/project/kiwisolver/)==1.4.5
- [langcodes](https://pypi.org/project/langcodes/)==3.3.0
- [lifelines](https://pypi.org/project/lifelines/)==0.27.8
- [llvmlite](https://pypi.org/project/llvmlite/)==0.41.1
- [lxml](https://pypi.org/project/lxml/)==5.1.0
- [MarkupSafe](https://pypi.org/project/MarkupSafe/)==2.1.3
- [matplotlib](https://pypi.org/project/matplotlib/)==3.8.1
- [matplotlib-inline](https://pypi.org/project/matplotlib-inline/)==0.1.6
- [medcat](https://pypi.org/project/medcat/)==1.9.3
- [mistune](https://pypi.org/project/mistune/)==3.0.2
- [mpmath](https://pypi.org/project/mpmath/)==1.3.0
- [multidict](https://pypi.org/project/multidict/)==6.0.4
- [multimethod](https://pypi.org/project/multimethod/)==1.10
- [multiprocess](https://pypi.org/project/multiprocess/)==0.70.15
- [murmurhash](https://pypi.org/project/murmurhash/)==1.0.10
- [nbclient](https://pypi.org/project/nbclient/)==0.8.0
- [nbconvert](https://pypi.org/project/nbconvert/)==7.10.0
- [nbformat](https://pypi.org/project/nbformat/)==5.9.2
- [nest-asyncio](https://pypi.org/project/nest-asyncio/)==1.5.8
- [networkx](https://pypi.org/project/networkx/)==3.2.1
- [nltk](https://pypi.org/project/nltk/)==3.8.1
- [notebook](https://pypi.org/project/notebook/)==7.0.6
- [notebook_shim](https://pypi.org/project/notebook-shim/)==0.2.3
- [numba](https://pypi.org/project/numba/)==0.58.1
- [numpy](https://pypi.org/project/numpy/)==1.23.5
- [nvidia-cublas-cu12](https://pypi.org/project/nvidia-cublas-cu12/)==12.1.3.1
- [nvidia-cuda-cupti-cu12](https://pypi.org/project/nvidia-cuda-cupti-cu12/)==12.1.105
- [nvidia-cuda-nvrtc-cu12](https://pypi.org/project/nvidia-cuda-nvrtc-cu12/)==12.1.105
- [nvidia-cuda-runtime-cu12](https://pypi.org/project/nvidia-cuda-runtime-cu12/)==12.1.105
- [nvidia-cufft-cu12](https://pypi.org/project/nvidia-cufft-cu12/)==11.0.2.54
- [nvidia-curand-cu12](https://pypi.org/project/nvidia-curand-cu12/)==10.3.2.106
- [nvidia-cusolver-cu12](https://pypi.org/project/nvidia-cusolver-cu12/)==11.4.5.107
- [nvidia-cusparse-cu12](https://pypi.org/project/nvidia-cusparse-cu12/)==12.1.0.106
- [nvidia-nvjitlink-cu12](https://pypi.org/project/nvidia-nvjitlink-cu12/)==12.3.52
- [nvidia-nvtx-cu12](https://pypi.org/project/nvidia-nvtx-cu12/)==12.1.105
- [openpyxl](https://pypi.org/project/openpyxl/)==3.1.2
- [overrides](https://pypi.org/project/overrides/)==7.4.0
- [packaging](https://pypi.org/project/packaging/)==23.2
- [pandas](https://pypi.org/project/pandas/)==1.5.3
- [pandas-profiling](https://pypi.org/project/pandas-profiling/)==3.6.6
- [pandocfilters](https://pypi.org/project/pandocfilters/)==1.5.0
- [paramiko](https://pypi.org/project/paramiko/)==3.3.1
- [parso](https://pypi.org/project/parso/)==0.8.3
- [patsy](https://pypi.org/project/patsy/)==0.5.6
- [pendulum](https://pypi.org/project/pendulum/)==2.1.2
- [pexpect](https://pypi.org/project/pexpect/)==4.8.0
- [phik](https://pypi.org/project/phik/)==0.12.4
- [Pillow](https://pypi.org/project/Pillow/)==10.1.0
- [platformdirs](https://pypi.org/project/platformdirs/)==3.11.0
- [polars](https://pypi.org/project/polars/)==0.19.12
- [preshed](https://pypi.org/project/preshed/)==3.0.9
- [prometheus-client](https://pypi.org/project/prometheus-client/)==0.18.0
- [prompt-toolkit](https://pypi.org/project/prompt-toolkit/)==3.0.43
- [psutil](https://pypi.org/project/psutil/)==5.9.6
- [ptyprocess](https://pypi.org/project/ptyprocess/)==0.7.0
- [pure-eval](https://pypi.org/project/pure-eval/)==0.2.2
- [py4j](https://pypi.org/project/py4j/)==0.10.9.7
- [pyarrow](https://pypi.org/project/pyarrow/)==14.0.0
- [pyarrow-hotfix](https://pypi.org/project/pyarrow-hotfix/)==0.6
- [pycparser](https://pypi.org/project/pycparser/)==2.21
- [pydantic](https://pypi.org/project/pydantic/)==1.10.15
- [pydantic-settings](https://pypi.org/project/pydantic-settings/)==2.1.0
- [pydantic_core](https://pypi.org/project/pydantic-core/)==2.18.1
- [Pygments](https://pypi.org/project/Pygments/)==2.16.1
- [pymetamap](https://pypi.org/project/pymetamap/)==0.1
- [PyNaCl](https://pypi.org/project/PyNaCl/)==1.5.0
- [pyodbc](https://pypi.org/project/pyodbc/)==5.0.1
- [pyparsing](https://pypi.org/project/pyparsing/)==3.1.1
- [python-dateutil](https://pypi.org/project/python-dateutil/)==2.8.2
- [python-dotenv](https://pypi.org/project/python-dotenv/)==1.0.0
- [python-json-logger](https://pypi.org/project/python-json-logger/)==2.0.7
- [python-pptx](https://pypi.org/project/python-pptx/)==0.6.23
- [pytz](https://pypi.org/project/pytz/)==2023.3.post1
- [pytzdata](https://pypi.org/project/pytzdata/)==2020.1
- [PyWavelets](https://pypi.org/project/PyWavelets/)==1.5.0
- [pywin32](https://pypi.org/project/pywin32/)==306
- [pywinpty](https://pypi.org/project/pywinpty/)==2.0.13
- [PyYAML](https://pypi.org/project/PyYAML/)==6.0.1
- [pyzmq](https://pypi.org/project/pyzmq/)==25.1.1
- [qtconsole](https://pypi.org/project/qtconsole/)==5.4.4
- [QtPy](https://pypi.org/project/QtPy/)==2.4.1
- [referencing](https://pypi.org/project/referencing/)==0.34.0
- [regex](https://pypi.org/project/regex/)==2023.10.3
- [requests](https://pypi.org/project/requests/)==2.31.0
- [rfc3339-validator](https://pypi.org/project/rfc3339-validator/)==0.1.4
- [rfc3986-validator](https://pypi.org/project/rfc3986-validator/)==0.1.1
- [rpds-py](https://pypi.org/project/rpds-py/)==0.10.6
- [safetensors](https://pypi.org/project/safetensors/)==0.4.2
- [scikit-learn](https://pypi.org/project/scikit-learn/)==1.3.2
- [scipy](https://pypi.org/project/scipy/)==1.9.3
- [seaborn](https://pypi.org/project/seaborn/)==0.12.2
- [Send2Trash](https://pypi.org/project/Send2Trash/)==1.8.2
- [six](https://pypi.org/project/six/)==1.16.0
- [smart-open](https://pypi.org/project/smart-open/)==6.4.0
- [sniffio](https://pypi.org/project/sniffio/)==1.3.0
- [soupsieve](https://pypi.org/project/soupsieve/)==2.5
- [spacy](https://pypi.org/project/spacy/)==3.7.2
- [spacy-legacy](https://pypi.org/project/spacy-legacy/)==3.0.12
- [spacy-loggers](https://pypi.org/project/spacy-loggers/)==1.0.5
- [srsly](https://pypi.org/project/srsly/)==2.4.8
- [stack-data](https://pypi.org/project/stack-data/)==0.6.3
- [statsmodels](https://pypi.org/project/statsmodels/)==0.14.1
- [sympy](https://pypi.org/project/sympy/)==1.12
- [tangled-up-in-unicode](https://pypi.org/project/tangled-up-in-unicode/)==0.2.0
- [terminado](https://pypi.org/project/terminado/)==0.17.1
- [thinc](https://pypi.org/project/thinc/)==8.2.1
- [threadpoolctl](https://pypi.org/project/threadpoolctl/)==3.2.0
- [tinycss2](https://pypi.org/project/tinycss2/)==1.2.1
- [tokenizers](https://pypi.org/project/tokenizers/)==0.15.2
- [tomli](https://pypi.org/project/tomli/)==2.0.1
- [torch](https://pypi.org/project/torch/)==2.1.0
- [tornado](https://pypi.org/project/tornado/)==6.3.3
- [tqdm](https://pypi.org/project/tqdm/)==4.66.1
- [traitlets](https://pypi.org/project/traitlets/)==5.13.0
- [transformers](https://pypi.org/project/transformers/)==4.39.3
- [typeguard](https://pypi.org/project/typeguard/)==4.1.5
- [typer](https://pypi.org/project/typer/)==0.9.0
- [types-python-dateutil](https://pypi.org/project/types-python-dateutil/)==2.8.19.14
- [typing_extensions](https://pypi.org/project/typing-extensions/)==4.8.0
- [tzdata](https://pypi.org/project/tzdata/)==2023.3
- [umls-api](https://pypi.org/project/umls-api/)==0.1.0
- [uri-template](https://pypi.org/project/uri-template/)==1.3.0
- [urllib3](https://pypi.org/project/urllib3/)==2.0.7
- [visions](https://pypi.org/project/visions/)==0.7.5
- [wasabi](https://pypi.org/project/wasabi/)==1.1.2
- [wcwidth](https://pypi.org/project/wcwidth/)==0.2.9
- [weasel](https://pypi.org/project/weasel/)==0.3.3
- [webcolors](https://pypi.org/project/webcolors/)==1.13
- [webencodings](https://pypi.org/project/webencodings/)==0.5.1
- [websocket-client](https://pypi.org/project/websocket-client/)==1.7.0
- [widgetsnbextension](https://pypi.org/project/widgetsnbextension/)==4.0.10
- [wordcloud](https://pypi.org/project/wordcloud/)==1.9.3
- [wrapt](https://pypi.org/project/wrapt/)==1.15.0
- [XlsxWriter](https://pypi.org/project/XlsxWriter/)==3.2.0
- [xxhash](https://pypi.org/project/xxhash/)==3.4.1
- [yarl](https://pypi.org/project/yarl/)==1.9.2
- [ydata-profiling](https://pypi.org/project/ydata-profiling/)==4.6.4
- [zipp](https://pypi.org/project/zipp/)==3.17.0
- [zope.interface](https://pypi.org/project/zope.interface/)==6.1


See util/requirements.txt

## Features:

- Single patient
- Batch patient
- Cohort search and creation
- Automated random controls
- Modular feature space selection
- Look back
- Look forward
- Individual patient time windows. 

## Installation

### Windows:

1. **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

    **Run the installation script:**
    ```shell
    install.bat
    ```

2. **Add the `pat2vec` directory to the Python path:**

   Before importing `pat2vec` in your Python script, add the following lines to the script, replacing `/path/to/pat2vec` with the actual path to the `pat2vec` directory inside your project:
   
    ```python
    import sys
    sys.path.append('/path/to/pat2vec')
    ```

3. **Import `pat2vec` in your Python script:**

    ```python
    import pat2vec
    ```

### Unix/Linux:

1. **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    ```
    
    . **Run the installation script:**
    
    ```shell
    (Requires python3 on path and venv)
    chmod +x install.sh
    ./install.sh
    ```
    
    cd pat2vec
    ```

2. **Add the `pat2vec` directory to the Python path:**

   Before importing `pat2vec` in your Python script, add the following lines to the script, replacing `/path/to/pat2vec` with the actual path to the `pat2vec` directory inside your project:
   
    ```python
    import sys
    sys.path.append('/path/to/pat2vec')
    ```

3. **Import `pat2vec` in your Python script:**

    ```python
    import pat2vec
    ```


## Usage:

- Set paths, gloabl_files/medcat_models/modelpack.zip, gloabl_files/snomed_methods, gloabl_files/..

- Configure options

- Run all


## Contributing
Contributions are welcome! Please see the contributing guidelines for more information.

## License
This project is licensed under the MIT License - see the LICENSE file for details

![Slide1](https://github.com/SamoraHunter/pat2vec/assets/44898312/f60dcf43-7fbe-4d96-8f33-9603694641b4)


![Slide2](https://github.com/SamoraHunter/pat2vec/assets/44898312/f93f47bb-46ad-4830-a010-4d6880a1bae6)


