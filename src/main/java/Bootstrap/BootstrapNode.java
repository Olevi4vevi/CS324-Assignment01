package Bootstrap;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Common.Constants;

/**
 * Entry point for the Bootstrap process. Runs standalone and must be started
 * before any Worker process. Creates the shared RMI registry that all workers
 * bind into.
 */
public class BootstrapNode {

    public static void main(String[] args) {
        int port = Constants.RMI_REGISTRY_PORT;

        try {
            Registry registry = LocateRegistry.createRegistry(port);
            BootstrapImpl bootstrap = new BootstrapImpl();
            registry.rebind(Constants.BOOTSTRAP_NAME, bootstrap);

            System.out.println("[Bootstrap] Ready on port " + port
                    + ", bound as \"" + Constants.BOOTSTRAP_NAME + "\"");
        } catch (RemoteException e) {
            System.err.println("[Bootstrap] Failed to start: " + e.getMessage());
        }
    }
}
