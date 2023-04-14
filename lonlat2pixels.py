def convert_geographic_coordinate_to_pixel_value(lon, lat, transform):
    """
    Converts a latitude/longitude coordinate to a pixel coordinate given the
    geotransform of the image.
    Args:
        lon: Pixel longitude.
        lat: Pixel latitude.
        transform: The geotransform array of the image.
    Returns:
        Tuple of refx, refy pixel coordinates.
    """

    xOrigin = transform[0]
    yOrigin = transform[3]
    pixelWidth = transform[1]
    pixelHeight = -transform[5]

    refx = round((lon - xOrigin) / pixelWidth)
    refy = round((yOrigin - lat) / pixelHeight)

    return refx, refy

example_array = [0.0, 5, 0.0, 0, 0.0, 5]


x = convert_geographic_coordinate_to_pixel_value(300, 200, example_array)