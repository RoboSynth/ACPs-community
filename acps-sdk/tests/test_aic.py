from acps_sdk.aic import (
    AICValidator,
    get_aic_segment,
    get_ontology_prefix_from_aic,
    is_entity_aic,
    is_ontology_aic,
    is_valid_aic_format,
    parse_aic,
    validate_aic_format,
)


def test_validate_aic_format_accepts_v02_order() -> None:
    aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
    valid, err = validate_aic_format(aic)
    assert valid is True
    assert err is None


def test_validate_aic_format_rejects_wrong_segment_count() -> None:
    aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546"
    valid, err = validate_aic_format(aic)
    assert valid is False
    assert err is not None
    assert "10 段" in err


def test_validate_aic_format_rejects_multichar_version() -> None:
    aic = "1.2.156.3088.11.1.34C2.478BDF.3GF546.0JU4"
    valid, err = validate_aic_format(aic)
    assert valid is False
    assert err is not None
    assert "第 5 级" in err


def test_parse_aic_maps_segments_correctly() -> None:
    aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
    info = parse_aic(aic)
    assert info is not None
    assert info.prefix == "1.2.156.3088"
    assert info.version == "1"
    assert info.arsp == "1"
    assert info.vendor == "34C2"
    assert info.ontology_serial == "478BDF"
    assert info.instance_serial == "3GF546"
    assert info.checksum == "0JU4"
    assert info.body == "1.2.156.3088.1.1.34C2.478BDF.3GF546"


def test_ontology_entity_helpers() -> None:
    ontology_aic = "1.2.156.3088.1.1.34C2.478BDF.000000.0SV9"
    entity_aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
    assert is_ontology_aic(ontology_aic) is True
    assert is_entity_aic(ontology_aic) is False
    assert is_entity_aic(entity_aic) is True
    assert is_ontology_aic(entity_aic) is False


def test_get_ontology_prefix_from_aic_uses_1_to_8_levels() -> None:
    entity_aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
    prefix = get_ontology_prefix_from_aic(entity_aic)
    assert prefix == "1.2.156.3088.1.1.34C2.478BDF"


def test_get_aic_segment() -> None:
    aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
    assert get_aic_segment(aic, 5) == "1"
    assert get_aic_segment(aic, 6) == "1"
    assert get_aic_segment(aic, 10) == "0JU4"
    assert get_aic_segment(aic, 11) is None


def test_validator_crc_with_salt_0x1234() -> None:
    body = "1.2.156.3088.1.1.34C2.478BDF.3GF546"
    full_aic = f"{body}.0JU4"

    validator = AICValidator(salt="0x1234")
    assert validator.calculate_checksum(body) == "0JU4"
    assert validator.validate(full_aic) is True

    invalid = f"{body}.ZZZZ"
    assert validator.validate(invalid) is False
    assert validator.last_error is not None


def test_validator_without_salt_behavior() -> None:
    aic = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
    validator = AICValidator()
    assert validator.validate(aic) is True
    assert validator.validate(aic, require_crc=True) is False


def test_is_valid_aic_format_bool_helper() -> None:
    assert is_valid_aic_format("1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4") is True
    assert is_valid_aic_format("1.2.156.3088.1.1.34C2.478BDF.3GF546") is False
