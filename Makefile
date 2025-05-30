include .env
export

.PHONY: initialize requirements install
       

# Installation and setup
initialize:
	pip install pip-tools
	python -m piptools compile requirements.in
	python -m piptools sync requirements.txt

requirements:
	python -m piptools compile requirements.in
	python -m piptools sync requirements.txt

install:
	python -m piptools sync requirements.txt