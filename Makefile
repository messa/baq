venv_dir=venv
python=python3

check: $(venv_dir)/packages-installed
	$(venv_dir)/bin/pytest -v tests

$(venv_dir)/packages-installed: requirements.txt requirements-tests.txt
	test -d $(venv_dir) || $(python) -m venv $(venv_dir)
	$(venv_dir)/bin/pip install -U pip
	$(venv_dir)/bin/pip install -r requirements.txt
	$(venv_dir)/bin/pip install -r requirements-tests.txt
	$(venv_dir)/bin/pip install -e .
	touch $@
