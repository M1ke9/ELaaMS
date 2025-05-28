package metrics;


import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

public class FunctionalitiesJMX {

    public static MBeanServerConnection connectToJMX(JMXServiceURL url) throws IOException {
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        return jmxc.getMBeanServerConnection();
    }

    public static Double getAttribute(MBeanServerConnection mbsc, ObjectName mbeanName,String attributeName) throws ReflectionException, InstanceNotFoundException, MBeanException, IOException {
        try {
            return  (Double) mbsc.getAttribute(mbeanName, attributeName);
        }catch (AttributeNotFoundException e){
            System.out.println("Attribute Not Found:"+attributeName);
            return 0.0;
        }

    }

}