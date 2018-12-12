"""
Function to test loading and pre-processing datasets
"""


def test_load_dataset():
    """
    Test loading the traffic datasets
    """
    from load_data import load_dataset
    import os
    zhanqian = load_dataset(os.getcwd() + '/../data/zhanqian.csv')
    huizhou = load_dataset(os.getcwd() + '/../data/zhonglou.csv')
    ningguo = load_dataset(os.getcwd() + '/../data/ningguo.csv')
    xiyou = load_dataset(os.getcwd() + '/../data/xiyou.csv')
    assert not zhanqian.empty
    assert not huizhou.empty
    assert not ningguo.empty
    assert not xiyou.empty

def test_load_rain():
    """
    Test loading the rain dataset
    """
    from load_data import load_rain_data
    import os
    zhanqian_rain = load_rain_data(os.getcwd() + '/../data/rain_data.xlsx', 2)
    huizhou_rain = load_rain_data(os.getcwd() + '/../data/rain_data.xlsx', 3)
    ningguo_rain = load_rain_data(os.getcwd() + '/../data/rain_data.xlsx', 4)
    xiyou_rain = load_rain_data(os.getcwd() + '/../data/rain_data.xlsx', 5)
    assert not zhanqian_rain.empty
    assert not huizhou_rain.empty
    assert not ningguo_rain.empty
    assert not xiyou_rain.empty