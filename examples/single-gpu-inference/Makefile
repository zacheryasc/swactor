.PHONY: venv clean-venv

VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python

venv: $(VENV_DIR)/bin/python

$(VENV_DIR)/bin/python:
	uv venv $(VENV_DIR) --python python3.11
	uv pip install --python $(VENV_DIR)/bin/python "tinygrad==0.12.0" numpy

clean-venv:
	rm -rf $(VENV_DIR)
