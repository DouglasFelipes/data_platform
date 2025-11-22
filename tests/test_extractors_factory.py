from data_platform.extractors.base_api import RestApiExtractor
from data_platform.extractors.factory import get_extractor
from data_platform.extractors.pdf_extractor import PdfExtractor


def test_get_extractor_known_types():
    cls = get_extractor("pdf")
    assert cls is PdfExtractor

    cls2 = get_extractor("rest_api")
    assert cls2 is RestApiExtractor


def test_get_extractor_unknown_type_raises():
    try:
        get_extractor("unknown_type")
        raised = False
    except ValueError:
        raised = True

    assert raised
