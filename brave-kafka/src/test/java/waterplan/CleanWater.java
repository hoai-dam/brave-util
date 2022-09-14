package waterplan;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CleanWater implements WaterCubic {

    final Boolean salty = false;
    final Boolean dirt = false;
    final Boolean contaminated = false;
    final Boolean heavyMetal = false;
    String characteristic;


    public CleanWater(WaterCubic waterCubic) {
        this.characteristic = waterCubic.getCharacteristic();
    }

}
