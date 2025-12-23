from typing import List, Dict, Any
from numpy import array, empty, float64

def to_numpy_book(book_list: List[Dict[Any, Any]]):
    if not book_list:
        return empty((0,2), dtype=float64)
    
    return array([[item['price'], item['size']] for item in book_list], dtype=float64)
    
