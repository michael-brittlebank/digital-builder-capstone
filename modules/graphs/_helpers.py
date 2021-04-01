import locale

locale.setlocale(locale.LC_ALL, '')


def formatter_currency(value, tick_number):
    """
    formatter_currency translates raw tick label to formatted currency
    :param value: plot value for an axis tick label
    :param tick_number: the index of the axis tick
    :return: axis tick label string
    """
    currency = locale.currency(value, grouping=True)
    currency_no_decimal = currency.split('.')[0]
    if currency_no_decimal.find(',') != -1:
        currency_no_decimal = currency_no_decimal[:currency_no_decimal.index(',')]
        if currency.count(',') == 1:
            currency_no_decimal += "K"
        elif currency.count(',') == 2:
            currency_no_decimal += "M"
    return currency_no_decimal


def formatter_percent(value, tick_number):
    """
    formatter_percent translates raw tick label to formatted percentage
    :param value: plot value for an axis tick label
    :param tick_number: the index of the axis tick
    :return: axis tick label string
    """
    return "{}%".format(int(value))
