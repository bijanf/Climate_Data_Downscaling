def calc_relative_humidity_from_dewpoint(dewp = None, ta = None):
    '''
    Calculates the relative humidity from dewpoint temperature given air temperature.

    Parameters
    ----------
    dewp : float or array
           Dewpoint temperature [K].
    ta : float or array
           Temperature [K].

    Returns
    -------
    hur : float or array
           Relative humidity [%]
    '''

    import numpy as __np
    #from .physics import calc_satvapp as __calc_satvapp
    #from .axis import comparison

    if ( dewp is None ):
        raise NameError("ERROR: no dewpoint temperature prescribed")
    if ( ta is None ):
        raise NameError("ERROR: no temperature prescribed")

    ta   = ta - 273.15
    dewp = dewp - 273.15

    K2         = __np.full_like(ta, 17.62)
    K3         = __np.full_like(ta, 243.12)
    K2[ta < 0] = 22.46
    K3[ta < 0] = 272.62

    A = (dewp * K2) / (K3 + ta) - (K2 * ta) / (K3 + ta)
    B = 1. + dewp / K3
    __hur = __np.exp( A / B ) * 100.

    __hur = __hur.values
    if __np.shape(__hur) == ():
        __hur = 0.01 if __hur < 0.01 else __hur
    else:
        __hur[__hur<0.01] = 0.01
    return __hur
