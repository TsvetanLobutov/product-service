package com.petsale.product_service.kafka;

import com.petsale.product_service.entity.Product;
import com.petsale.product_service.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
@Component
public class OrderConsumer {

    private final ProductRepository productRepository;

    @KafkaListener(topics = "order-events", groupId = "product-group")
    public void consumeOrderEvent(String message) {
        log.info("Received Kafka message: {}", message);

        try {
            // Parse the JSON string (for a learning project, simple split is fine)
            Long productId = Long.parseLong(message.split("\"productId\": ")[1].split(",")[0]);
            Integer quantityOrdered = Integer.parseInt(message.split("\"quantity\": ")[1].split(",")[0]);

            // Find the product in the database
            Optional<Product> optionalProduct = productRepository.findById(productId);

            if (optionalProduct.isPresent()) {
                Product product = optionalProduct.get();
                int currentStock = product.getStockQuantity();

                if (currentStock >= quantityOrdered) {
                    // Reduce the stock
                    product.setStockQuantity(currentStock - quantityOrdered);
                    productRepository.save(product);
                    log.info("Successfully reduced stock for product ID {}. New stock: {}", productId, product.getStockQuantity());
                } else {
                    log.warn("Not enough stock for product ID {}. Current: {}, Ordered: {}", productId, currentStock, quantityOrdered);
                    // In a real app, you would trigger a rollback or send an email here
                }
            } else {
                log.error("Product with ID {} not found!", productId);
            }

        } catch (Exception e) {
            log.error("Error processing Kafka message: {}", message, e);
        }
    }
}