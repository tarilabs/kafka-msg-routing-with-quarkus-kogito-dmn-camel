package org.drools.demo;

import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.ClaimCheckOperation;
import org.kie.kogito.decision.DecisionModels;

import io.connectedhealth_idaas.eventbuilder.parsers.clinical.HL7RoutingEventParser;
import utils.camel3.DMNEvaluateAllKogitoProcessor;
import utils.camel3.DMNResultDecisionsToHeadersProcessor;

@ApplicationScoped
public class IDAASDREAMRouteBuilder extends RouteBuilder {

    @Inject
    DecisionModels kogitoDMNModels;

    static final Processor bodyToMap = (Exchange e) -> e.getIn().setBody(Map.of("event", e.getIn().getBody()));
    static final Processor decisionsToHeaders = DMNResultDecisionsToHeadersProcessor.builder("topicsHeader", "topic names").build();

    @Override
    public void configure() throws Exception {
        final Processor kogitoDMNEvaluate = new DMNEvaluateAllKogitoProcessor(kogitoDMNModels.getDecisionModel("ns1", "RoutingEvent"));

        from("direct:hl7")
            .bean(HL7RoutingEventParser.class, "buildRoutingEvent(${body})")
            .claimCheck(ClaimCheckOperation.Push)
                .process(bodyToMap)
                .process(kogitoDMNEvaluate)
                .process(decisionsToHeaders)
            .claimCheck(ClaimCheckOperation.Pop)
            .to("log:org.drools.demo?level=DEBUG&showAll=true&multiline=true")
            .transform().simple("${body.messageData}")
            .choice()
            .when(simple("${header.topicsHeader.size} > 0"))
                .loop(simple("${header.topicsHeader.size}"))
                    .setHeader(KafkaConstants.OVERRIDE_TOPIC, 
                        simple("${header.topicsHeader[${exchangeProperty.CamelLoopIndex}]}"))
                    .to("kafka:default")
                .endChoice()
            .otherwise()
                .to("kafka:CATCH_ALL")
            ;
    }
}
