
def get_emissions(distance,mode,journeytype): # all conversion values taken from google for the year 2022-23 
    if mode == 'Plane':
        co2 = distance * 0.194028
    elif mode =='Tube':
        co2 = distance * 0.02781
    elif mode == 'Metro':
        co2 = distance * 0.02813
    elif mode == 'Tram':
        co2 = distance * 0.02813
    elif mode == 'Bus':
        co2 = distance * 0.081008
    elif mode == 'Train':
        co2 = distance * 0.03549
    elif mode == 'Taxi':
        co2 = distance * 0.17646
    elif mode == 'Motorbike':
        co2 = distance * 0.10749
    else:
        co2 = distance * 0.144996

    if journeytype == 'Return':
        return float((co2*2))
    else:
        return float(co2)


def cost(emissions):
    """Computes cost from emissions. This is based on the cost of offsetting per tCO2e from
    ClimateCare."""
    return round(emissions / 1000 * 8.5, 2)