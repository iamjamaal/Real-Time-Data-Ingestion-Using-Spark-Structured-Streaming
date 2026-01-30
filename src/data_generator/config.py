  # Configuration parameters for event generation

CONFIG = {
    'output_dir': '/data/streaming',
    'events_per_file': 100,
    'generation_interval_seconds': 30,
    'user_pool_size': 1000,  # Number of unique users
    
    # Event type probabilities
    'event_distribution': {
        'view': 0.60,      # 60% browsing
        'cart_add': 0.25,  # 25% add to cart
        'purchase': 0.10,  # 10% actual purchase
        'search': 0.05     # 5% search events
    },
    
    # Product configuration
    'num_products': 100,
    'product_categories': ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'],
    'price_range': (9.99, 999.99),
    'price_variation': 0.2,  # +/- 20%
    
    # Data quality controls
    'null_probability': 0.01,  # 1% null values (for testing)
    'duplicate_probability': 0.005  # 0.5% duplicates
}