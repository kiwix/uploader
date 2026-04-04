default:
  just --list

lint_args := ""
lintfix_args := ""
check_args := ""
test_args := ""
coverage_args := ""

lint args=lint_args:
  ruff check {{args}}

fix args=lintfix_args:
  ruff check --fix {{args}}

check args=check_args:
  pyright {{args}}

test args=test_args:
  pytest {{args}}

coverage args=coverage_args:
  coverage run -m pytest {{args}}
  coverage combine
  coverage report --show-missing
  coverage xml
  coverage html

debug:
  ipython
