def calculate_bollinger_bands(data, period=20, std_multiplier=2):
    """Calculate Bollinger Bands for the given data.
    
    Args:
        data (pd.DataFrame): DataFrame containing price data
        period (int): Period for moving average (default: 20)
        std_multiplier (float): Number of standard deviations (default: 2)
    
    Returns:
        tuple: (middle_band, upper_band, lower_band)
    """
    # Calculate middle band (simple moving average)
    middle_band = data['close'].rolling(window=period).mean()
    
    # Calculate standard deviation
    std = data['close'].rolling(window=period).std()
    
    # Calculate upper and lower bands
    upper_band = middle_band + (std * std_multiplier)
    lower_band = middle_band - (std * std_multiplier)
    
    return middle_band, upper_band, lower_band