import sys
import pathlib
import importlib

# Ensure the package parent directory is on sys.path so relative imports inside
# `esl_processor` (like `from .models import ...`) resolve correctly.
PACKAGE_DIR = pathlib.Path(__file__).parent
PARENT_DIR = PACKAGE_DIR.parent
sys.path.insert(0, str(PARENT_DIR))

esl_pkg = importlib.import_module("esl_processor.data_mapper")
DataMapper = osl = getattr(esl_pkg, "DataMapper")


class DummyLogger:
    def log(self, *args, **kwargs):
        pass


import pytest


@pytest.fixture()
def mapper():
    return DataMapper(db_manager=None, agency_id_code="AG", logger=DummyLogger())


def test_valid_with_time(mapper):
    filename = "file_description_REPORT_250312_2359"
    is_compliant, parts = mapper._check_eim_compliance(filename)

    assert is_compliant is True
    assert parts["description"] == "FILE_DESCRIPTION"
    assert parts["title"] == "REPORT"
    assert parts["date"] == "20250312"
    assert parts.get("time") == "2359"


def test_valid_with_pdf_extension(mapper):
    filename = "STMT_THOMPSON_250522.pdf"
    is_compliant, parts = mapper._check_eim_compliance(filename)

    assert is_compliant is True
    assert parts["description"] == "STMT"
    assert parts["title"] == "THOMPSON"
    assert parts["date"] == "20250522"


def test_valid_without_time(mapper):
    filename = "case_notes_LAB_240101"
    is_compliant, parts = mapper._check_eim_compliance(filename)

    assert is_compliant is True
    assert parts["description"] == "CASE_NOTES"
    assert parts["title"] == "LAB"
    assert parts["date"] == "20240101"
    assert "time" not in parts


def test_valid_with_8digit_date(mapper):
    filename = "DOCUMENT_STATEMENT_20240308_1430"
    is_compliant, parts = mapper._check_eim_compliance(filename)

    assert is_compliant is True
    assert parts["description"] == "DOCUMENT"
    assert parts["title"] == "STATEMENT"
    assert parts["date"] == "20240308"
    assert parts.get("time") == "1430"


@pytest.mark.parametrize(
    "filename",
    [
        "ONLY_TWO",
        "desc_TITLE_250332",        # invalid day
        "desc_TITLE_250312_2460",  # invalid time
        "desc_REPORT_(250312)_(1230)",
    ],
)
def test_invalid_user_filenames(filename, mapper):
    is_compliant, parts = mapper._check_eim_compliance(filename)

    assert is_compliant is False
    assert parts["description"] == filename
    assert parts["title"] == ""
    assert parts["date"] == ""
