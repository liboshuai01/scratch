package cn.liboshuai.scratch.flink.mini;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class StreamInputProcessor implements MailboxDefaultAction{

    private final MiniInputGate inputGate;
    private final DataOutput output;

    public StreamInputProcessor(MiniInputGate inputGate, DataOutput output) {
        this.inputGate = inputGate;
        this.output = output;
    }

    @Override
    public void runDefaultAction(Controller controller) {
        NetworkBuffer buffer = inputGate.pollNext();
        if (buffer != null) {
            output.processRecord(buffer.toStringContent());
        } else {
            CompletableFuture<Void> availabilityFuture = inputGate.getAvailabilityFuture();
            if (availabilityFuture.isDone()) {
                return;
            }
            controller.suspendDefaultAction();
            availabilityFuture.thenRun(() -> {
                ((MailboxProcessor) controller).resumeDefaultAction();
            });
        }
    }
}
