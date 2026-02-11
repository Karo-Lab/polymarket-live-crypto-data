from numpy import array, empty, float64

def to_numpy_book(book_list: list):
    if not book_list:
        return empty((2, 0), dtype=float64)
    
    prices = [float(item['price']) for item in book_list]
    sizes = [float(item['size']) for item in book_list]
    
    return array([prices, sizes], dtype=float64)
    
