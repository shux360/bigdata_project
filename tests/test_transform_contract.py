def test_net_grid_formula():
    consumption, solar = 2.0, 3.0
    assert max(0.0, consumption-solar) == 0.0

