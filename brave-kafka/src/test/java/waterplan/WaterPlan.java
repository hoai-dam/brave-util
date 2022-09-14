package waterplan;

import brave.kafka.Streams;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@Streams(
        bootstrapServers = "localhost:9092",
        applicationId = "waterplan-general-test",
        pollMillis = 200,
        numStreamThreads = 2
)
public class WaterPlan {

    public static final String GROUND_WATER = "ground-water";
    public static final String SEA_WATER = "sea-water";
    public static final String SURFACE_WATER = "surface-water";
    public static final String SEA_WATER_DEPOSITION_TANK = "sea-water-deposition-tank";
    public static final String INLAND_WATER_DEPOSITION_TANK = "inland-water-deposition-tank";
    public static final String DESALINATION_TANK = "desalination-tank";
    public static final String HEAVY_METAL_REDUCTION_TANK = "heavyMetalReduction-tank";
    public static final String DECONTAMINATION_TANK = "decontamination-tank";
    public static final String CIVIL_WATER_TANK = "civil-water-tank";
    public static final String INDUSTRY_WATER_TANK = "industry-water-tank";
    public static final String REDISTRIBUTE_TANK = "redistribute-tank";

    ConcurrentHashMap<String, Integer> waterVolumes = new ConcurrentHashMap<>();
    KafkaStreams kafkaStreams;


    public Map<String, Integer> getWaterVolumes() {
        return Collections.unmodifiableMap(waterVolumes);
    }

    @Streams.Inject
    public void setKafkaStreams(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public KafkaStreams.State getState() {
        return kafkaStreams.state();
    }

    @Streams.Topology
    public Topology topology() {
        Topology topology = new Topology();

        topology.addSource(GROUND_WATER, new GroundWaterDeserializer(), new GroundWaterDeserializer(),
                "waterplan.suburban.ground_water", "waterplan.mountain.ground_water");

        topology.addSource(SEA_WATER, new SeaWaterDeserializer(), new SeaWaterDeserializer(),
                "waterplan.north_coastline.sea_water", "waterplan.south_coastline.sea_water");

        topology.addSource(SURFACE_WATER, new SurfaceWaterDeserializer(), new SurfaceWaterDeserializer(),
                "waterplan.red_river.surface_water", "waterplan.green_river.surface_water");

        topology.addProcessor(SEA_WATER_DEPOSITION_TANK, SeaWaterDeposition::new, SEA_WATER)
                .addProcessor(INLAND_WATER_DEPOSITION_TANK, InlandWaterDeposition::new, GROUND_WATER, SURFACE_WATER)
                .addProcessor(DESALINATION_TANK, Desalination::new, SEA_WATER_DEPOSITION_TANK)
                .addProcessor(HEAVY_METAL_REDUCTION_TANK, HeavyMetalReduction::new, DESALINATION_TANK, INLAND_WATER_DEPOSITION_TANK)
                .addProcessor(DECONTAMINATION_TANK, Decontamination::new, HEAVY_METAL_REDUCTION_TANK);

        topology.addSink(CIVIL_WATER_TANK, "waterplan.civil_water_tank",
                new CleanWaterSerializer(), new CleanWaterSerializer(), DECONTAMINATION_TANK);

        topology.addSink(INDUSTRY_WATER_TANK, "waterplan.industry_water_tank",
                new NoHeavyMetalSerializer(), new NoHeavyMetalSerializer(), HEAVY_METAL_REDUCTION_TANK);

        topology.addSink(REDISTRIBUTE_TANK, "waterplan.redistribute_tank",
                new NoHeavyMetalSerializer(), new NoHeavyMetalSerializer(), HEAVY_METAL_REDUCTION_TANK);

        return topology;
    }


    class SeaWaterDeposition extends ContextualProcessor<WaterCubic, WaterCubic, NoDirtWater, NoDirtWater> {

        @Override
        public void process(Record<WaterCubic, WaterCubic> record) {
            increaseCount(SEA_WATER_DEPOSITION_TANK);

            Record<NoDirtWater, NoDirtWater> noDirt = new Record<>(
                    new NoDirtWater(record.key()),
                    new NoDirtWater(record.value()),
                    System.currentTimeMillis()
            );

            context().forward(noDirt);
        }
    }

    class InlandWaterDeposition extends ContextualProcessor<WaterCubic, WaterCubic, NoSaltWater, NoSaltWater> {

        @Override
        public void process(Record<WaterCubic, WaterCubic> record) {
            increaseCount(INLAND_WATER_DEPOSITION_TANK);

            Record<NoSaltWater, NoSaltWater> noSalt = new Record<>(
                    new NoSaltWater(record.key()),
                    new NoSaltWater(record.value()),
                    System.currentTimeMillis()
            );

            context().forward(noSalt);
        }
    }

    class Desalination extends ContextualProcessor<WaterCubic, WaterCubic, NoSaltWater, NoSaltWater> {

        @Override
        public void process(Record<WaterCubic, WaterCubic> record) {
            increaseCount(DESALINATION_TANK);

            Record<NoSaltWater, NoSaltWater> noSalt = new Record<>(
                    new NoSaltWater(record.key()),
                    new NoSaltWater(record.value()),
                    System.currentTimeMillis()
            );

            context().forward(noSalt);
        }
    }

    class HeavyMetalReduction extends ContextualProcessor<WaterCubic, WaterCubic, NoHeavyMetalWater, NoHeavyMetalWater> {

        @Override
        public void process(Record<WaterCubic, WaterCubic> record) {
            int newCount = increaseCount(HEAVY_METAL_REDUCTION_TANK);

            Record<NoHeavyMetalWater, NoHeavyMetalWater> noHeavyMetal = new Record<>(
                    new NoHeavyMetalWater(record.key()),
                    new NoHeavyMetalWater(record.value()),
                    System.currentTimeMillis()
            );

            if (newCount % 3 == 0) {
                context().forward(noHeavyMetal, DECONTAMINATION_TANK);
            } else if (newCount % 3 == 1) {
                context().forward(noHeavyMetal, INDUSTRY_WATER_TANK);
                increaseCount(INDUSTRY_WATER_TANK);
            } else {
                context().forward(noHeavyMetal, REDISTRIBUTE_TANK);
                increaseCount(REDISTRIBUTE_TANK);
            }
        }
    }

    class Decontamination extends ContextualProcessor<WaterCubic, WaterCubic, CleanWater, CleanWater> {

        @Override
        public void process(Record<WaterCubic, WaterCubic> record) {
            increaseCount(DECONTAMINATION_TANK);

            Record<CleanWater, CleanWater> cleanWater = new Record<>(
                    new CleanWater(record.key()),
                    new CleanWater(record.value()),
                    System.currentTimeMillis()
            );

            context().forward(cleanWater);
            increaseCount(CIVIL_WATER_TANK);
        }
    }

    private Integer increaseCount(String key) {
        return waterVolumes.compute(key, (String nodeName, Integer count) -> (count == null ? 0 : count) + 1);
    }

}
