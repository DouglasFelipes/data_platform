from data_platform.extractors.base_api import RestApiExtractor
from data_platform.extractors.factory import get_extractor
from data_platform.extractors.fnde_pdf import FndePdfExtractor


def test_get_extractor_known_types():
    cls = get_extractor("fnde_pdf")
    assert cls is FndePdfExtractor

    cls2 = get_extractor("rest_api")
    assert cls2 is RestApiExtractor


def test_get_extractor_unknown_type_raises():
    try:
        get_extractor("unknown_type")
        raised = False
    except ValueError:
        raised = True

    assert raised
