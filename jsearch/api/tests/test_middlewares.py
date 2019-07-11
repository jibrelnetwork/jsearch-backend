import pytest

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


@pytest.mark.parametrize(
    ('name', 'expected'),
    (
        ('Access-Control-Allow-Headers', '*'),
        ('Access-Control-Allow-Origin', '*'),
        ('Access-Control-Request-Method', 'POST, GET, OPTIONS, HEAD'),
    )
)
async def test_cors_headers_are_present(cli, name, expected):
    response = await cli.get('/v1/accounts/x')
    response_header = response.headers[name]

    assert response_header == expected
