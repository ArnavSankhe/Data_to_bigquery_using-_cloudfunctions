
def get_emissions(distance, mode, journeytype):
    """
    Calculates the amount of CO2 emissions based on the distance traveled, the mode of transportation, and the type of journey.

    Args:
        distance (float): The distance traveled in kilometers.
        mode (string): The mode of transportation. Possible values are 'Plane', 'Tube', 'Metro', 'Tram', 'Bus', 'Train', 'Taxi', and 'Motorbike'.
        journeytype (string): The type of journey. Possible values are 'Return' or any other value.

    Returns:
        float: The amount of CO2 emissions in metric tons.
    """
    if journeytype == 'Return':
        distance *= 2

    emission_factors = {
        'Plane': 0.194028,
        'Tube': 0.02781,
        'Metro': 0.02813,
        'Tram': 0.02813,
        'Bus': 0.081008,
        'Train': 0.03549,
        'Taxi': 0.17646,
        'Motorbike': 0.10749
    }

    co2 = distance * emission_factors.get(mode, 0.144996)

    return float(co2)


def cost(emissions):
    """
    Computes the cost of offsetting emissions based on the amount of emissions in tCO2e.

    # existing code for the 'cost' function
    pass
    Parameters:
    emissions (float): The amount of emissions in tCO2e.

    Returns:
    float: The cost of offsetting the emissions.

    Example:
    >>> cost(5000)
    42.5
    """
    return round(emissions / 1000 * 8.5, 2)