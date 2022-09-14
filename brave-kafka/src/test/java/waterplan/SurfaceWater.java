package waterplan;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SurfaceWater implements WaterCubic {

    final Boolean salty = false;
    final Boolean dirt = true;
    Boolean contaminated = true;
    Boolean heavyMetal = true;
    String characteristic;

}
