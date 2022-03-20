LAYER_VERSION = $(shell aws lambda list-layer-versions --layer-name layer_sflib | grep LayerVersionArn |sort -r | head -1 | awk '{print $$2}' | sed -e 's/[",]//g')

all: lambda_function.zip lambda_layer.zip

lambda_function.zip: lambda_function.py api.yaml
	zip -r lambda_function.zip lambda_function.py api.yaml

install_function: lambda_function.zip
	aws lambda update-function-code --function-name hello \
	    --zip-file fileb://lambda_function.zip

	aws lambda update-function-configuration --function-name hello \
	    --layers $(LAYER_VERSION) \
	    --timeout 30

lambda_layer.zip:
	rm -rf python
	mkdir -p python
	pip install --target python snowflake-connector-python

	mkdir -p python/sflib
	cp sflib.py python/sflib

	zip -r lambda_layer.zip python

install_layer: lambda_layer.zip
	aws lambda publish-layer-version \
	    --layer-name layer_sflib \
	    --zip-file fileb://lambda_layer.zip \
	    --compatible-runtimes python3.8

install: install_layer install_function

clean:
	rm -rf lambda_function.zip lambda_layer.zip
	rm -rf *~ python __pycache__
