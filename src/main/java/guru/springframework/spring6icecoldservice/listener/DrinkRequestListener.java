package guru.springframework.spring6icecoldservice.listener;


import guru.springframework.spring6icecoldservice.config.KafkaConfig;
import guru.springframework.spring6icecoldservice.service.DrinkRequestProcessor;
import guru.springframework.spring6restmvcapi.events.DrinkPreparedEvent;
import guru.springframework.spring6restmvcapi.events.DrinkRequestEvent;
import guru.springframework.spring6restmvcapi.model.BeerOrderDTO;
import guru.springframework.spring6restmvcapi.model.BeerOrderLineDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class DrinkRequestListener {

    private final DrinkRequestProcessor drinkRequestProcessor;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(groupId = "IceColdListener", topics = KafkaConfig.DRINK_REQUEST_ICE_COLD_TOPIC)
    public void listenDrinkRequest(DrinkRequestEvent event) {
        System.out.println("I am listening - drink request");

        drinkRequestProcessor.processDrinkRequest(event);
        kafkaTemplate.send(KafkaConfig.DRINK_PREPARED_TOPIC, DrinkPreparedEvent.builder()
                .beerOrderDTO(BeerOrderDTO.builder()
                        .beerOrderLines((Set<BeerOrderLineDTO>) event.getBeerOrderLineDTO())
                        .build()
                        ).build());
    }
}
