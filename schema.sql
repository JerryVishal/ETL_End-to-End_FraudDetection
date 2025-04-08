
-- USERS TABLE
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    location VARCHAR(100),
    signup_date DATE NOT NULL
);

-- PRODUCTS TABLE
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INT NOT NULL CHECK (stock >= 0)
);

-- TRANSACTIONS TABLE
CREATE TABLE transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(50) CHECK (payment_method IN ('Credit Card', 'Debit Card', 'PayPal', 'Crypto', 'Bank Transfer')),
    status VARCHAR(20) CHECK (status IN ('Success', 'Failed', 'Pending')),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- USER INTERACTIONS TABLE
CREATE TABLE user_interactions (
    interaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    action VARCHAR(50) CHECK (action IN ('Viewed', 'Added to Cart', 'Wishlist', 'Purchased')),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- PAYMENTS TABLE
CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(20) UNIQUE NOT NULL,
    payment_method VARCHAR(50) CHECK (payment_method IN ('Credit Card', 'Debit Card', 'PayPal', 'Crypto', 'Bank Transfer')),
    card_type VARCHAR(20) CHECK (card_type IN ('Visa', 'MasterCard', 'Amex', 'None')),
    status VARCHAR(20) CHECK (status IN ('Success', 'Failed', 'Pending')),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) ON DELETE CASCADE
);

-- INDEXES FOR PERFORMANCE OPTIMIZATION
CREATE INDEX idx_transactions_user ON transactions(user_id);
CREATE INDEX idx_transactions_product ON transactions(product_id);
CREATE INDEX idx_interactions_user ON user_interactions(user_id);
CREATE INDEX idx_interactions_product ON user_interactions(product_id);
CREATE INDEX idx_payments_transaction ON payments(transaction_id);

-- CONFIRMATION
SELECT 'E-commerce schema created successfully!' AS confirmation;