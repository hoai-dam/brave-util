package garden;

import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Seed {

    @EqualsAndHashCode.Include
    private String name;

}
