
ZIP_NAME = wazuh_to_amazon_security_lake
TARGET = package
SRC = src

# Main target
.ONESHELL:
$(ZIP_NAME).zip: $(TARGET) $(SRC)/lambda_function.py $(SRC)/wazuh_ocsf_converter.py
	@cd $(TARGET)
	@zip -r ../$(ZIP_NAME).zip .
	@cd ../$(SRC)
	@zip ../$@ lambda_function.py wazuh_ocsf_converter.py
	@zip ../$@ models -r

$(TARGET):
	docker run -v `pwd`:/src -w /src \
		python:3.12 \
		pip install \
		--platform manylinux2014_x86_64 \
		--target=$(TARGET) \
		--implementation cp \
		--python-version 3.12 \
		--only-binary=:all: \
		-r requirements.aws.txt

clean:
	@rm -rf $(TARGET)
	docker run -v `pwd`:/src -w /src \
		python:3.12 \
		py3clean .