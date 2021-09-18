package org.mpisws.hitmc.zookeeper;

import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.server.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;

public class ZookeeperMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMain.class);

    public static void main(final String[] args) {
        final ApplicationContext applicationContext = new AnnotationConfigApplicationContext(ZookeeperSpringConfig.class);
        final Scheduler scheduler = applicationContext.getBean(Scheduler.class);

        try {
            scheduler.loadConfig(args);
            scheduler.start();
            Thread.sleep(Long.MAX_VALUE);
//            System.exit(0);
        } catch (final SchedulerConfigurationException e) {
            LOG.error("Error while reading configuration.", e);
        } catch (final IOException e) {
            LOG.error("IO exception", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
