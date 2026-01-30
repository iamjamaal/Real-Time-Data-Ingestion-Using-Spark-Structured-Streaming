# data generator 


import os
import time
import csv
from datetime import datetime, timedelta
from faker import Faker
import random
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class EcommerceEventGenerator:
    """
    Generates realistic e-commerce events as CSV files.
    Events include user interactions like views, cart additions, and purchases.
    """
    
    def __init__(self, output_dir: str = '/data/streaming', 
                 events_per_file: int = 100,
                 generation_interval: int = 30):
        self.output_dir = output_dir
        self.events_per_file = events_per_file
        self.generation_interval = generation_interval
        self.fake = Faker()
        
        # Product catalog
        self.products = [
            {'id': f'PROD{i:04d}', 'name': self.fake.catch_phrase(), 
             'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports']),
             'base_price': round(random.uniform(9.99, 999.99), 2)}
            for i in range(1, 101)
        ]
        
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Generator initialized: {events_per_file} events per file, {generation_interval}s interval")
    
    def generate_event(self, user_pool_size: int = 1000) -> Dict:
        """
        Generate a single e-commerce event.
        
        Event types:
        - view: 60% probability (browsing)
        - cart_add: 25% probability (consideration)
        - purchase: 10% probability (conversion)
        - search: 5% probability
        """
        event_type_distribution = {
            'view': 0.60,
            'cart_add': 0.25,
            'purchase': 0.10,
            'search': 0.05
        }
        
        event_type = random.choices(
            list(event_type_distribution.keys()),
            weights=list(event_type_distribution.values())
        )[0]
        
        product = random.choice(self.products)
        user_id = f"USER{random.randint(1, user_pool_size):05d}"
        
        
        
        # Price variation (+/- 20% for sales/markups)
        price_multiplier = random.uniform(0.8, 1.2)
        final_price = round(product['base_price'] * price_multiplier, 2)
        
        event = {
            'user_id': user_id,
            'session_id': self.fake.uuid4(),
            'event_type': event_type,
            'product_id': product['id'],
            'product_name': product['name'],
            'product_category': product['category'],
            'price': final_price,
            'quantity': random.randint(1, 5) if event_type == 'purchase' else 1,
            'timestamp': datetime.now().isoformat(),
            'user_agent': self.fake.user_agent(),
            'ip_address': self.fake.ipv4(),
            'country': self.fake.country(),
            'city': self.fake.city()
        }
        
        return event
    
    
    def generate_file(self, file_number: int):
        """Generate a single CSV file with events."""
        filename = f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_number:04d}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        events = [self.generate_event() for _ in range(self.events_per_file)]
        
        with open(filepath, 'w', newline='') as csvfile:
            fieldnames = events[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(events)
        
        logger.info(f"Generated: {filename} ({len(events)} events)")
        return filepath
    
    
    def run_continuous(self, duration_minutes: int = None):
        """
        Run generator continuously.
        
        Args:
            duration_minutes: Run for specified duration (None = infinite)
        """
        logger.info("Starting continuous event generation...")
        start_time = datetime.now()
        file_counter = 0
        
        try:
            while True:
                self.generate_file(file_counter)
                file_counter += 1
                
                if duration_minutes:
                    elapsed = (datetime.now() - start_time).total_seconds() / 60
                    if elapsed >= duration_minutes:
                        logger.info(f"Completed {duration_minutes} minute run")
                        break
                
                time.sleep(self.generation_interval)
                
        except KeyboardInterrupt:
            logger.info(f"Generation stopped. Created {file_counter} files")
            
            

if __name__ == "__main__":
    generator = EcommerceEventGenerator(
        output_dir=os.getenv('OUTPUT_DIR', '/data/streaming'),
        events_per_file=int(os.getenv('EVENTS_PER_FILE', 100)),
        generation_interval=int(os.getenv('GENERATION_INTERVAL', 30))
    )
    generator.run_continuous()