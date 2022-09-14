package waterplan;

import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SeaWater implements WaterCubic {

    final Boolean salty = true;
    final Boolean dirt = true;
    Boolean contaminated = true;
    Boolean heavyMetal = false;
    String characteristic;

}
