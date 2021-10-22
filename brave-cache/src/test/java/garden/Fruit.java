package garden;

import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Fruit {

    @EqualsAndHashCode.Include
    private String name;

    @EqualsAndHashCode.Include
    private int generation;

    public Fruit(String name) {
        this.name = name;
        this.generation = 0;
    }

    public Fruit(Seed seed) {
        this.name = seed.getName();
        this.generation = 0;
    }

    public Fruit(Seed seed, int generation) {
        this.name = seed.getName();
        this.generation = generation;
    }

}
