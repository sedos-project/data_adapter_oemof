PYTHONPATH := $(CURDIR)
export PYTHONPATH

download_collection:
	python -c "from data_adapter import main; main.download_collection('$(collection)')"
