import io.envoyproxy.envoy.config.cluster.v3.Cluster;

public class TestClusterApiCheck {
    public static void main(String[] args) {
        Cluster.Builder builder = Cluster.newBuilder();
        try {
            // Check if the method exists
            java.lang.reflect.Method method = Cluster.class.getMethod("getLrsReportEndpointMetricsList");
            System.out.println("Method exists: " + method.getName());
            
            // Also check for the getCount and isEmpty methods
            java.lang.reflect.Method countMethod = Cluster.class.getMethod("getLrsReportEndpointMetricsCount");
            System.out.println("Count method exists: " + countMethod.getName());
            
        } catch (NoSuchMethodException e) {
            System.out.println("Method does not exist: " + e.getMessage());
            
            // Let's see what methods are available
            System.out.println("Available methods containing 'Lrs':");
            for (java.lang.reflect.Method method : Cluster.class.getMethods()) {
                if (method.getName().toLowerCase().contains("lrs")) {
                    System.out.println("  " + method.getName());
                }
            }
        }
    }
} 