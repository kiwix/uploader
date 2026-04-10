default:
  just --list

lint_args := ""
lintfix_args := ""
check_args := ""
test_args := ""
coverage_args := ""
runner := env('PYRUN', '')

lint args=lint_args:
  {{runner}} ruff check {{args}}

fix args=lintfix_args:
  {{runner}} ruff check --fix {{args}}

check args=check_args:
  {{runner}} pyright {{args}}

test args=test_args:
  {{runner}} pytest {{args}}

coverage args=coverage_args:
  {{runner}} coverage run -m pytest {{args}}
  {{runner}} coverage combine
  {{runner}} coverage report --show-missing
  {{runner}} coverage xml
  {{runner}} coverage html

debug:
  {{runner}} ipython

version:
  {{runner}} python3 -V
