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

}
