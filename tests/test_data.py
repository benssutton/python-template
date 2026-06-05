
async def test_get_shape(test_client):
    """ N.B. - the path to the ipc stream data file is defined in
        the Settings class and has been overwritten to ./test_data
        which has a different shape
    """
    response = await test_client.get("/data/shape")
    j = response.json()
    assert response.status_code == 200
    assert  j["height"] == 3
    assert  j["width"] == 2
