venv_dir=venv
python=python3

check: $(venv_dir)/packages-installed
	$(venv_dir)/bin/pytest -v tests
	make lint

lint: $(venv_dir)/packages-installed
	test -x $(venv_dir)/bin/flake8 || $(venv_dir)/bin/pip install flake8
	$(venv_dir)/bin/flake8 . --show-source --statistics

$(venv_dir)/packages-installed: requirements.txt requirements-tests.txt
	test -d $(venv_dir) || $(python) -m venv $(venv_dir)
	$(venv_dir)/bin/pip install -U pip
	$(venv_dir)/bin/pip install -r requirements.txt
	$(venv_dir)/bin/pip install -r requirements-tests.txt
	$(venv_dir)/bin/pip install -e .
	touch $@
