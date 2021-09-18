package org.apache.zookeeper.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public aspect WorkerReceiverAspect {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerReceiverAspect.class);

    private Integer lastSentMessageId = null;

    private int workerReceiverSubnodeId;

    // Keep track of the message we're replying to
    private QuorumCnxManager.Message response;

    public QuorumCnxManager.Message getResponse() {
        return response;
    }

    private final AtomicInteger msgsInRecvQueue = new AtomicInteger(0);

    public AtomicInteger getMsgsInRecvQueue() {
        return msgsInRecvQueue;
    }

    // Intercept starting the thread

    pointcut runWorkerReceiver(): execution(* FastLeaderElection.Messenger.WorkerReceiver.run());

    before(): runWorkerReceiver() {
        final FastLeaderElectionAspect fleAspect = FastLeaderElectionAspect.aspectOf();
        workerReceiverSubnodeId = fleAspect.registerWorkerReceiverSubnode();
    }

    after(): runWorkerReceiver() {
        final FastLeaderElectionAspect fleAspect = FastLeaderElectionAspect.aspectOf();
        fleAspect.deregisterWorkerReceiverSubnode(workerReceiverSubnodeId);
    }

    // Intercept message offering within WorkerReceiver

    pointcut offerWithinWorkerReceiver(Object object):
            within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* java.util.concurrent.LinkedBlockingQueue.offer(Object))
            && if (object instanceof FastLeaderElection.ToSend)
            && args(object);

    before(final Object object): offerWithinWorkerReceiver(object) {
        final FastLeaderElection.ToSend toSend = (FastLeaderElection.ToSend) object;

        final Set<Integer> predecessorIds = new HashSet<>();
        predecessorIds.add(response.getMessageId());
        if (null != lastSentMessageId) {
            predecessorIds.add(lastSentMessageId);
        }

        final FastLeaderElectionAspect fleAspect = FastLeaderElectionAspect.aspectOf();
        try {
            LOG.debug("WorkerReceiver subnode {} is offering a message with predecessors {}", workerReceiverSubnodeId, predecessorIds.toString());
            fleAspect.setWorkerReceiverSending();
            final String payload = fleAspect.constructPayload(toSend);
            lastSentMessageId = fleAspect.getScheduler().offerMessage(workerReceiverSubnodeId, (int) toSend.sid, predecessorIds, payload);
            LOG.debug("Scheduler returned id = {}", lastSentMessageId);
            fleAspect.workerReceiverPostSend(workerReceiverSubnodeId, lastSentMessageId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept forwarding a notification to FLE.recvqueue

    pointcut forwardNotification(Object object):
            within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* java.util.concurrent.LinkedBlockingQueue.offer(Object))
            && if (object instanceof FastLeaderElection.Notification)
            && args(object);

    before(final Object object): forwardNotification(object) {
        final FastLeaderElectionAspect fleAspect = FastLeaderElectionAspect.aspectOf();
        try {
            fleAspect.getScheduler().setProcessingState(fleAspect.getFleSubnodeId());
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept polling the receive queue of the QuorumCnxManager

    pointcut pollRecvQueue():
            within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* QuorumCnxManager.pollRecvQueue(..));

    before(): pollRecvQueue() {
        final FastLeaderElectionAspect fleAspect = FastLeaderElectionAspect.aspectOf();
        if (msgsInRecvQueue.get() == 0) {
            // Going to block here. Better notify the scheduler
            LOG.debug("My QCM.recvQueue is empty, go to RECEIVING state");
            try {
                fleAspect.getScheduler().setReceivingState(workerReceiverSubnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after() returning (final QuorumCnxManager.Message response): pollRecvQueue() {
        if (null != response) {
            final FastLeaderElectionAspect fleAspect = FastLeaderElectionAspect.aspectOf();
            LOG.debug("Received a message with id = {}", response.getMessageId());
            msgsInRecvQueue.decrementAndGet();
            this.response = response;
        }
    }
}
