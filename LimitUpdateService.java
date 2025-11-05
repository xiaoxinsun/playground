package com.workflow.limit;

// ... existing imports ...
import io.quarkus.logging.Log; // Make sure Log is imported
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.TimeoutException;

// Imports for Fault Tolerance
import org.eclipse.microprofile.faulttolerance.Retry;
import java.time.temporal.ChronoUnit;

// A placeholder for your downstream API client
record CustomerLimit(String customerId, double amount) {}

@ApplicationScoped
public class LimitUpdateService {

    @Inject
    LockService lockService; // <-- Inject the INTERFACE

    // ... existing mock API methods ...
    private CustomerLimit getLimitFromApi(String customerId) {
        Log.infof("Calling GET /limit/%s", customerId);
        // ... external HTTP call ...
        // Faking a 100ms API response
        try { Thread.sleep(100); } catch (Exception e) {}
        return new CustomerLimit(customerId, 100.0); // Faking existing limit
    }

    // In-memory "mock" of your downstream API
    private void updateLimitInApi(String customerId, double newAmount) {
        Log.infof("Calling UPDATE /limit/%s with amount %f", customerId, newAmount);
        // ... external HTTP call ...
        // Faking a 100ms API response
        try { Thread.sleep(100); } catch (Exception e) {}
        Log.info("Update complete.");
    }


    /**
     * This is the public method your API controller would call.
     * It uses the lock service to protect the read-modify-write operation.
     *
     * We've added @Retry to automatically handle lock contention (TimeoutException).
     */
    @Retry(
        maxRetries = 4, // 1 initial attempt + 4 retries = 5 total
        delay = 50,     // Initial delay
        delayUnit = ChronoUnit.MILLIS,
        maxDuration = 2000, // Max time to spend retrying
        durationUnit = ChronoUnit.MILLIS,
        jitter = 20,    // Add/subtract 20ms of random jitter
        jitterDelayUnit = ChronoUnit.MILLIS,
        retryOn = {TimeoutException.class} // Only retry on this exception
    )
    public CustomerLimit addLimitToCustomer(String customerId, double amountToAdd) throws Exception {

        // The logic inside this method is now CLEAN.
        // The @Retry annotation wraps this method in a proxy
        // that handles the try/catch/wait/jitter logic for us.

        try {
            // We pass the entire unsafe operation as a lambda to the lock service
            return lockService.tryWithLock(customerId, () -> {

                // --- START OF PROTECTED CODE ---
                // Only one thread per customerId can be in here at a time.
                Log.infof("Lock acquired for %s. Proceeding with R-M-W.", customerId);

                // 1. GET current limit
                CustomerLimit existingLimit = getLimitFromApi(customerId);

                // 2. Calculate new limit
                double newAmount = existingLimit.amount() + amountToAdd;

                // 3. UPDATE new limit
                updateLimitInApi(customerId, newAmount);

                // --- END OF PROTECTED CODE ---

                return new CustomerLimit(customerId, newAmount);
            });

        } catch (TimeoutException e) {
            // This exception will be caught on EACH attempt that fails to get a lock.
            // The @Retry proxy will catch the re-thrown exception and try again.
            Log.warnf("Concurrent update for %s failed this attempt. Retrying if possible...", customerId);
            // Re-throw it so the @Retry annotation's proxy can catch it and handle the retry.
            throw e;
        } catch (Exception e) {
            // If the API call itself failed (e.g., 500 from downstream),
            // we do NOT retry, because it's not a TimeoutException.
            Log.error("Failed to update limit due to non-retryable error", e);
            throw e;
        }
    }
}
