package waterplan;

import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class GroundWater implements WaterCubic {

    final Boolean salty = false;
    final Boolean dirt = true;
    Boolean contaminated = true;
    Boolean heavyMetal = true;
    String characteristic;

}
