import pytest
from cloaca.api.shared import fetch_ebird_taxonomy_with_cache


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_fetch_ebird_taxonomy_with_cache():
    taxonomy = await fetch_ebird_taxonomy_with_cache()

    eastern_phoebe = taxonomy["easpho"]

    # {\"sciName\":\"Sayornis
    # phoebe\",\"comName\":\"Eastern Phoebe\",\"speciesCode\":\"easpho\",\"category\":\"species\",\"taxonOrder\":16840.0,\"bandingCodes\":[\"EAPH\"],\"comNameCodes\":[],\"sciNameCodes\":[\"SAPH\"],\"order\":\"Passeriformes\",\"familyCode\":\"tyrann2\",\"familyComName\":\"Tyrant
    # Flycatchers\",\"familySciName\":\"Tyrannidae\"}
    assert eastern_phoebe is not None

    assert eastern_phoebe.banding_codes == ["EAPH"]
    assert eastern_phoebe.com_name == "Eastern Phoebe"
    assert eastern_phoebe.order == "Passeriformes"
    assert eastern_phoebe.family_com_name == "Tyrant Flycatchers"
    assert eastern_phoebe.family_sci_name == "Tyrannidae"
