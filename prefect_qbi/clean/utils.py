import random
import re
import time


def convert_to_snake_case(text):
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z])([A-Z])", r"\1_\2", text)
    return text.lower()


def get_unique_temp_table_name(base_name):
    """Generate a unique temporary table name with timestamp and random number"""
    timestamp = int(time.time())
    random_suffix = random.randint(1000, 9999)
    return f"{base_name}__temp_{timestamp}_{random_suffix}"
