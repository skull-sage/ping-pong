package com.pingpong.ping.application;

import com.pingpong.ping.application.port.in.RecordPongCommand;
import com.pingpong.ping.application.port.in.RecordPongUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Closes the loop: logs the returning pong. Kept trivial — the point is the trace correlation. */
@Service
public class RecordPongService implements RecordPongUseCase {

    private static final Logger log = LoggerFactory.getLogger(RecordPongService.class);

    @Override
    public void record_pong(RecordPongCommand command) {
        log.info("Pong received from {} for pingId={} corr={}",
                command.responder(), command.pingId(), command.correlationId());
    }
}
